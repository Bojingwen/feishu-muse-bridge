// feishubridge: Feishu (Lark) <-> Muse bridge daemon.
//
// Purpose: the user talks to Muse *himself* from Feishu (group or DM).
// No third-party LLM is involved anywhere in this program.
//
// Flow:
//   Feishu group --(@bot, long-connection WS)--> this daemon
//     --> bridge-data/inbox/<message_id>.json
//     --> a Muse hook notices the file and wakes a Muse worker
//     --> Muse writes bridge-data/outbox/<message_id>.json
//     --> this daemon updates its "thinking" placeholder in Feishu
//         with Muse's reply (or sends a fresh reply as fallback)
//
// The daemon uses the Feishu long-connection mode (larkws), so it needs
// no public IP / callback URL. Anti-loop: events whose sender_type is
// "app" are dropped, and every message_id this daemon sends is recorded
// and dropped if it ever echoes back.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	"github.com/larksuite/oapi-sdk-go/v3/event/dispatcher"
	larkapplication "github.com/larksuite/oapi-sdk-go/v3/service/application/v6"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
	larkws "github.com/larksuite/oapi-sdk-go/v3/ws"
)

type Config struct {
	LarkAppID     string `json:"lark_app_id"`
	LarkAppSecret string `json:"lark_app_secret"`
	BotName       string `json:"bot_name,omitempty"` // fallback if the app-info API is not permitted
}

type InboxMsg struct {
	ID         string `json:"id"` // feishu message_id
	ChatID     string `json:"chat_id"`
	ChatType   string `json:"chat_type"` // group | p2p
	SenderID   string `json:"sender_id"`
	SenderName string `json:"sender_name,omitempty"`
	Text       string `json:"text"`
	ReceivedAt string `json:"received_at"`
}

type Attachment struct {
	Path string `json:"path"`           // absolute path under ~/workspace
	Kind string `json:"kind,omitempty"` // image | video | file (inferred from extension when empty)
}

type OutboxMsg struct {
	ReplyTo     string       `json:"reply_to"` // original feishu message_id
	ChatID      string       `json:"chat_id"`
	Text        string       `json:"text"`
	Attachments []Attachment `json:"attachments,omitempty"`
}

var (
	rootDir   string
	botName   string
	larkCli   *lark.Client
	sentIDs   sync.Map // message_id -> time.Time, anti-loop registry
	pendingMu sync.Mutex
	pending   = map[string]string{} // original message_id -> placeholder message_id
)

// pendingPath is where the placeholder map is persisted. The map must
// survive daemon restarts: without it, a restart leaves the chat's
// "thinking…" placeholder stuck forever and replies arrive as separate
// messages (a restart mid-conversation used to strand them).
func pendingPath() string { return filepath.Join(rootDir, "pending.json") }

func loadPending() {
	data, err := os.ReadFile(pendingPath())
	if err != nil {
		return
	}
	m := map[string]string{}
	if err := json.Unmarshal(data, &m); err == nil {
		pendingMu.Lock()
		pending = m
		pendingMu.Unlock()
		logf("loaded %d pending placeholder(s) from disk", len(m))
	}
}

// savePending persists the placeholder map. Callers must hold pendingMu.
func savePending() {
	data, err := json.Marshal(pending)
	if err != nil {
		return
	}
	tmp := pendingPath() + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err == nil {
		os.Rename(tmp, pendingPath())
	}
}

func logf(format string, args ...interface{}) {
	fmt.Printf("%s "+format+"\n", append([]interface{}{time.Now().Format("2006-01-02 15:04:05")}, args...)...)
}

func markSent(id string) {
	if id != "" {
		sentIDs.Store(id, time.Now())
	}
}

func isSelfSent(id string) bool {
	v, ok := sentIDs.Load(id)
	if !ok {
		return false
	}
	if t, ok := v.(time.Time); ok && time.Since(t) > 10*time.Minute {
		sentIDs.Delete(id)
		return false
	}
	return true
}

func appendHistory(chatID, role, text string) {
	if chatID == "" {
		return
	}
	rec, _ := json.Marshal(map[string]interface{}{
		"ts": time.Now().Format(time.RFC3339), "role": role, "text": text,
	})
	f, err := os.OpenFile(filepath.Join(rootDir, "history", chatID+".jsonl"),
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer f.Close()
	f.Write(append(rec, '\n'))
}

func markdownPost(text string) string {
	content, _ := larkim.NewMessagePost().ZhCn(
		larkim.NewMessagePostContent().AppendContent(
			[]larkim.MessagePostElement{&postMarkdown{Text: text}},
		).Build()).Build()
	return content
}

type postMarkdown struct{ Text string }

func (m *postMarkdown) Tag() string  { return "md" }
func (m *postMarkdown) IsPost()      {}
func (m *postMarkdown) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{"tag": "md", "text": m.Text})
}

// extractText returns (cleanText, atBot) for text and post messages.
func extractText(msg *larkim.P2MessageReceiveV1) (string, bool) {
	if msg.Event == nil || msg.Event.Message == nil {
		return "", false
	}
	m := msg.Event.Message
	atBot := false
	var mentionKey string
	for _, at := range m.Mentions {
		if botName != "" && larkcore.StringValue(at.Name) == botName {
			atBot = true
			mentionKey = larkcore.StringValue(at.Key)
		}
	}
	if len(m.Mentions) > 0 && botName == "" {
		atBot = true // bot name unknown: any mention counts
	}
	switch larkcore.StringValue(m.MessageType) {
	case larkim.MsgTypeText:
		var body struct {
			Text string `json:"text"`
		}
		if err := json.Unmarshal([]byte(larkcore.StringValue(m.Content)), &body); err != nil {
			return "", atBot
		}
		text := body.Text
		if mentionKey != "" {
			text = strings.ReplaceAll(text, mentionKey, "")
		}
		if botName != "" {
			// Some clients send "@botname" as plain text with no mentions
			// array; count that as an @ too.
			if strings.Contains(text, "@"+botName) {
				atBot = true
			}
			text = strings.ReplaceAll(text, "@"+botName, "")
		}
		return strings.TrimSpace(text), atBot
	case larkim.MsgTypePost:
		var post struct {
			Content [][]struct {
				Tag  string `json:"tag"`
				Text string `json:"text"`
			} `json:"content"`
		}
		if err := json.Unmarshal([]byte(larkcore.StringValue(m.Content)), &post); err != nil {
			return "", atBot
		}
		var parts []string
		for _, line := range post.Content {
			for _, el := range line {
				if el.Tag == "text" {
					parts = append(parts, el.Text)
				}
			}
		}
		return strings.TrimSpace(strings.Join(parts, " ")), atBot
	}
	return "", atBot
}

func onMessage(ctx context.Context, msg *larkim.P2MessageReceiveV1) error {
	if msg == nil || msg.Event == nil || msg.Event.Message == nil {
		return nil
	}
	// Anti-loop layer 1: never react to app/bot-sent messages.
	if msg.Event.Sender != nil && larkcore.StringValue(msg.Event.Sender.SenderType) == "app" {
		logf("anti-loop: dropped app-sent event")
		return nil
	}
	msgID := larkcore.StringValue(msg.Event.Message.MessageId)
	// Anti-loop layer 2: never react to our own sent message_ids echoing back.
	if isSelfSent(msgID) {
		logf("anti-loop: dropped own message echo %s", msgID)
		return nil
	}
	if msg.Event.Sender == nil || msg.Event.Sender.SenderId == nil {
		logf("dropped event without sender id")
		return nil
	}

	text, atBot := extractText(msg)
	chatType := larkcore.StringValue(msg.Event.Message.ChatType)
	if chatType == "group" && !atBot {
		return nil // group chats: only when @'d
	}
	if strings.TrimSpace(text) == "" {
		return nil // nothing to answer (sticker/image-only etc.)
	}

	chatID := larkcore.StringValue(msg.Event.Message.ChatId)
	senderID := larkcore.StringValue(msg.Event.Sender.SenderId.OpenId)
	if senderID == "" {
		senderID = larkcore.StringValue(msg.Event.Sender.SenderId.UserId)
	}

	in := InboxMsg{
		ID: msgID, ChatID: chatID, ChatType: chatType, SenderID: senderID,
		Text: text, ReceivedAt: time.Now().Format(time.RFC3339),
	}
	data, _ := json.MarshalIndent(in, "", "  ")
	tmp := filepath.Join(rootDir, "inbox", "."+msgID+".tmp")
	final := filepath.Join(rootDir, "inbox", msgID+".json")
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		logf("write inbox fail: %v", err)
		return nil
	}
	if err := os.Rename(tmp, final); err != nil {
		logf("rename inbox fail: %v", err)
		return nil
	}
	appendHistory(chatID, "user", text)
	logf("inbox <- %s chat=%s text=%q", msgID, chatID, text)

	// Placeholder reply so the group sees Muse is on it; the outbox
	// watcher later updates this very message with the real answer.
	resp, err := larkCli.Im.Message.Reply(ctx, larkim.NewReplyMessageReqBuilder().
		MessageId(msgID).
		Body(larkim.NewReplyMessageReqBodyBuilder().
			MsgType(larkim.MsgTypePost).
			Content(markdownPost("Muse 正在思考…")).
			Build()).
		Build())
	if err == nil && resp.Success() && resp.Data != nil && resp.Data.MessageId != nil {
		markSent(*resp.Data.MessageId)
		pendingMu.Lock()
		pending[msgID] = *resp.Data.MessageId
		savePending()
		pendingMu.Unlock()
	} else {
		logf("placeholder reply fail: err=%v resp=%v", err, resp)
	}
	return nil
}

// watchOutbox polls for Muse's replies and delivers them to Feishu.
func watchOutbox(ctx context.Context) {
	outDir := filepath.Join(rootDir, "outbox")
	sentDir := filepath.Join(rootDir, "sent")
	for {
		entries, _ := filepath.Glob(filepath.Join(outDir, "*.json"))
		sort.Strings(entries)
		for _, path := range entries {
			data, err := os.ReadFile(path)
			if err != nil {
				continue
			}
			var out OutboxMsg
			if err := json.Unmarshal(data, &out); err != nil || (out.Text == "" && len(out.Attachments) == 0) {
				logf("bad outbox file %s: %v", path, err)
				os.Rename(path, path+".bad")
				continue
			}
			deliver(ctx, out)
			stamp := time.Now().Format("20060102-150405")
			os.Rename(path, filepath.Join(sentDir, stamp+"-"+filepath.Base(path)))
		}
		time.Sleep(2 * time.Second)
	}
}

func deliver(ctx context.Context, out OutboxMsg) {
	pendingMu.Lock()
	placeholder, hasPlaceholder := pending[out.ReplyTo]
	if hasPlaceholder {
		delete(pending, out.ReplyTo)
		savePending()
	}
	pendingMu.Unlock()

	text := out.Text
	if text == "" && len(out.Attachments) > 0 {
		text = "附件如下："
	}

	if hasPlaceholder {
		resp, err := larkCli.Im.Message.Update(ctx, larkim.NewUpdateMessageReqBuilder().
			MessageId(placeholder).
			Body(larkim.NewUpdateMessageReqBodyBuilder().
				MsgType(larkim.MsgTypePost).
				Content(markdownPost(text)).
				Build()).
			Build())
		if err == nil && resp.Success() {
			appendHistory(out.ChatID, "assistant", text)
			logf("outbox -> updated placeholder for %s", out.ReplyTo)
			deliverAttachments(ctx, out)
			return
		}
		logf("update placeholder fail, falling back to reply: err=%v resp=%v", err, resp)
	}
	// Fallback: reply to the original message.
	resp, err := larkCli.Im.Message.Reply(ctx, larkim.NewReplyMessageReqBuilder().
		MessageId(out.ReplyTo).
		Body(larkim.NewReplyMessageReqBodyBuilder().
			MsgType(larkim.MsgTypePost).
			Content(markdownPost(text)).
			Build()).
		Build())
	if err == nil && resp.Success() {
		if resp.Data != nil && resp.Data.MessageId != nil {
			markSent(*resp.Data.MessageId)
		}
		appendHistory(out.ChatID, "assistant", text)
		logf("outbox -> replied to %s", out.ReplyTo)
		deliverAttachments(ctx, out)
		return
	}
	// Last resort: post a fresh message into the chat.
	resp2, err2 := larkCli.Im.Message.Create(ctx, larkim.NewCreateMessageReqBuilder().
		ReceiveIdType(larkim.ReceiveIdTypeChatId).
		Body(larkim.NewCreateMessageReqBodyBuilder().
			MsgType(larkim.MsgTypePost).
			ReceiveId(out.ChatID).
			Content(markdownPost(text)).
			Build()).
		Build())
	if err2 == nil && resp2.Success() {
		if resp2.Data != nil && resp2.Data.MessageId != nil {
			markSent(*resp2.Data.MessageId)
		}
		appendHistory(out.ChatID, "assistant", text)
		logf("outbox -> created message in chat %s", out.ChatID)
		deliverAttachments(ctx, out)
		return
	}
	logf("deliver FAILED for %s: reply err=%v, create err=%v", out.ReplyTo, err, err2)
}

// --- attachments: upload to Feishu, then send as replies ---

func attachmentKind(a Attachment) string {
	if a.Kind != "" {
		return a.Kind
	}
	switch strings.ToLower(filepath.Ext(a.Path)) {
	case ".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp":
		return "image"
	case ".mp4", ".mov", ".avi", ".mkv":
		return "video"
	}
	return "file"
}

func feishuFileType(path string) string {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".mp4":
		return "mp4"
	case ".pdf":
		return "pdf"
	case ".doc", ".docx":
		return "doc"
	case ".xls", ".xlsx":
		return "xls"
	case ".ppt", ".pptx":
		return "ppt"
	case ".opus":
		return "opus"
	}
	return "stream"
}

func uploadImage(ctx context.Context, path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	resp, err := larkCli.Im.Image.Create(ctx, larkim.NewCreateImageReqBuilder().
		Body(larkim.NewCreateImageReqBodyBuilder().ImageType("message").Image(f).Build()).
		Build())
	if err != nil {
		return "", err
	}
	if !resp.Success() {
		return "", fmt.Errorf("image upload code=%d msg=%s", resp.Code, resp.Msg)
	}
	return larkcore.StringValue(resp.Data.ImageKey), nil
}

func uploadFile(ctx context.Context, path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	resp, err := larkCli.Im.File.Create(ctx, larkim.NewCreateFileReqBuilder().
		Body(larkim.NewCreateFileReqBodyBuilder().
			FileType(feishuFileType(path)).
			FileName(filepath.Base(path)).
			File(f).
			Build()).
		Build())
	if err != nil {
		return "", err
	}
	if !resp.Success() {
		return "", fmt.Errorf("file upload code=%d msg=%s", resp.Code, resp.Msg)
	}
	return larkcore.StringValue(resp.Data.FileKey), nil
}

// prepareVideo makes a Feishu-client-friendly copy of an mp4: it remuxes
// with the moov atom at the front (+faststart, required by several mobile
// clients or the video shows as unplayable) and extracts a cover frame.
// Returns (pathToSend, coverPath); on any ffmpeg failure it falls back to
// the original file and no cover.
func prepareVideo(src string) (string, string) {
	ffmpeg, err := exec.LookPath("ffmpeg")
	if err != nil {
		return src, ""
	}
	dir := filepath.Join(rootDir, "tmp-video")
	os.MkdirAll(dir, 0755)
	base := strings.TrimSuffix(filepath.Base(src), filepath.Ext(src))
	fast := filepath.Join(dir, base+"-faststart.mp4")
	if out, err := exec.Command(ffmpeg, "-y", "-v", "error", "-i", src,
		"-c", "copy", "-movflags", "+faststart", fast).CombinedOutput(); err != nil {
		logf("faststart remux fail (%s): %v %s", src, err, string(out))
		return src, ""
	}
	cover := filepath.Join(dir, base+"-cover.jpg")
	if err := exec.Command(ffmpeg, "-y", "-v", "error", "-i", fast,
		"-frames:v", "1", "-q:v", "3", cover).Run(); err != nil {
		logf("cover extract fail (%s): %v", src, err)
		return fast, ""
	}
	return fast, cover
}

func deliverAttachments(ctx context.Context, out OutboxMsg) {
	home, _ := os.UserHomeDir()
	allowedPrefix := filepath.Join(home, "workspace") + string(os.PathSeparator)
	for _, a := range out.Attachments {
		kind := attachmentKind(a)
		if !strings.HasPrefix(a.Path, allowedPrefix) {
			logf("attachment refused (outside workspace): %s", a.Path)
			continue
		}
		var msgType, content string
		var err error
		switch kind {
		case "image":
			var key string
			key, err = uploadImage(ctx, a.Path)
			if err == nil {
				b, _ := json.Marshal(map[string]string{"image_key": key})
				msgType, content = larkim.MsgTypeImage, string(b)
			}
		case "video":
			sendPath, coverPath := prepareVideo(a.Path)
			var key string
			key, err = uploadFile(ctx, sendPath)
			if err == nil {
				body := map[string]string{"file_key": key}
				// Feishu clients show a black/unplayable tile without a
				// cover image_key; attach the extracted first frame.
				if coverPath != "" {
					if ikey, ierr := uploadImage(ctx, coverPath); ierr == nil && ikey != "" {
						body["image_key"] = ikey
					} else {
						logf("video cover upload fail: %v", ierr)
					}
				}
				b, _ := json.Marshal(body)
				msgType, content = larkim.MsgTypeMedia, string(b)
			}
		default: // file
			var key string
			key, err = uploadFile(ctx, a.Path)
			if err == nil {
				b, _ := json.Marshal(map[string]string{"file_key": key})
				msgType, content = larkim.MsgTypeFile, string(b)
			}
		}
		if err != nil {
			logf("attachment upload FAILED %s: %v", a.Path, err)
			continue
		}
		resp, rerr := larkCli.Im.Message.Reply(ctx, larkim.NewReplyMessageReqBuilder().
			MessageId(out.ReplyTo).
			Body(larkim.NewReplyMessageReqBodyBuilder().MsgType(msgType).Content(content).Build()).
			Build())
		if rerr == nil && resp.Success() {
			if resp.Data != nil && resp.Data.MessageId != nil {
				markSent(*resp.Data.MessageId)
			}
			appendHistory(out.ChatID, "assistant", "["+kind+"] "+filepath.Base(a.Path))
			logf("attachment sent: %s (%s)", a.Path, kind)
			continue
		}
		// Fallback: create in chat.
		resp2, cerr := larkCli.Im.Message.Create(ctx, larkim.NewCreateMessageReqBuilder().
			ReceiveIdType(larkim.ReceiveIdTypeChatId).
			Body(larkim.NewCreateMessageReqBodyBuilder().MsgType(msgType).ReceiveId(out.ChatID).Content(content).Build()).
			Build())
		if cerr == nil && resp2.Success() {
			if resp2.Data != nil && resp2.Data.MessageId != nil {
				markSent(*resp2.Data.MessageId)
			}
			appendHistory(out.ChatID, "assistant", "["+kind+"] "+filepath.Base(a.Path))
			logf("attachment sent (create): %s (%s)", a.Path, kind)
		} else {
			logf("attachment send FAILED %s: reply err=%v create err=%v", a.Path, rerr, cerr)
		}
	}
}

func main() {
	confPath := flag.String("conf", "", "json config with lark_app_id / lark_app_secret")
	root := flag.String("root", "", "bridge-data root directory")
	flag.Parse()

	home, _ := os.UserHomeDir()
	if *root == "" {
		*root = filepath.Join(home, "workspace", "feishu-bridge", "bridge-data")
	}
	rootDir = *root
	for _, sub := range []string{"inbox", "outbox", "claimed", "done", "sent", "history"} {
		os.MkdirAll(filepath.Join(rootDir, sub), 0755)
	}
	if *confPath == "" {
		*confPath = filepath.Join(home, "workspace", "feishu-bridge", "config", "bridge.json")
	}
	data, err := os.ReadFile(*confPath)
	if err != nil {
		logf("FATAL: read config %s: %v", *confPath, err)
		os.Exit(1)
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		logf("FATAL: parse config: %v", err)
		os.Exit(1)
	}
	if cfg.LarkAppID == "" || cfg.LarkAppSecret == "" ||
		strings.Contains(cfg.LarkAppID, "xxxx") || strings.Contains(cfg.LarkAppSecret, "REPLACE") {
		logf("FATAL: lark_app_id / lark_app_secret not filled in %s", *confPath)
		os.Exit(2)
	}

	loadPending()

	ctx := context.Background()
	larkCli = lark.NewClient(cfg.LarkAppID, cfg.LarkAppSecret, lark.WithLogger(stdLogger{}))

	// Learn the bot's display name so group @ detection works.
	if resp, err := larkCli.Application.Application.Get(ctx,
		larkapplication.NewGetApplicationReqBuilder().AppId(cfg.LarkAppID).Lang("zh_cn").Build()); err == nil && resp.Success() {
		botName = larkcore.StringValue(resp.Data.App.AppName)
		logf("bot name: %s", botName)
	} else if cfg.BotName != "" {
		botName = cfg.BotName
		logf("bot name from config: %s (app-info API not permitted)", botName)
	} else {
		logf("WARN: get bot name failed (group @ detection falls back to any-mention): err=%v", err)
	}

	go watchOutbox(ctx)

	handler := dispatcher.NewEventDispatcher("", "").OnP2MessageReceiveV1(onMessage)
	ws := larkws.NewClient(cfg.LarkAppID, cfg.LarkAppSecret,
		larkws.WithEventHandler(handler),
		larkws.WithLogLevel(larkcore.LogLevelInfo),
		larkws.WithLogger(stdLogger{}),
	)
	logf("starting Feishu long-connection…")
	if err := ws.Start(ctx); err != nil {
		logf("FATAL: ws start: %v", err)
		os.Exit(1)
	}
}

type stdLogger struct{}

func (stdLogger) Debug(ctx context.Context, args ...interface{}) { fmt.Println(append([]interface{}{"DEBUG"}, args...)...) }
func (stdLogger) Info(ctx context.Context, args ...interface{})  { fmt.Println(append([]interface{}{"INFO"}, args...)...) }
func (stdLogger) Warn(ctx context.Context, args ...interface{})  { fmt.Println(append([]interface{}{"WARN"}, args...)...) }
func (stdLogger) Error(ctx context.Context, args ...interface{}) { fmt.Println(append([]interface{}{"ERROR"}, args...)...) }
