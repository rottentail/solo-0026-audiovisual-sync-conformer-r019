package main

import (
	"archive/zip"
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rottentail/solo-0026-audiovisual-sync-conformer/internal/domain"
	"github.com/rottentail/solo-0026-audiovisual-sync-conformer/internal/httpapi"
	"github.com/rottentail/solo-0026-audiovisual-sync-conformer/internal/media"
	appservice "github.com/rottentail/solo-0026-audiovisual-sync-conformer/internal/service"
	"github.com/rottentail/solo-0026-audiovisual-sync-conformer/internal/storage"
)

type checkContext struct {
	root      string
	server    *httptest.Server
	client    *http.Client
	audio     []byte
	video     []byte
	metaAudio []byte
	metaVideo []byte
}

func main() {
	workflow := flag.String("workflow", "", "workflow to exercise")
	flag.Parse()
	if *workflow == "" {
		fatalf("workflow is required")
	}
	ctx, err := newCheckContext()
	if err != nil {
		fatalf("prepare check: %v", err)
	}
	defer ctx.server.Close()
	defer os.RemoveAll(ctx.root)

	switch *workflow {
	case "admit-take":
		err = runAdmitCheck(ctx)
	case "analyze-sync":
		err = runAnalyzeCheck(ctx)
	case "render-conform":
		err = runRenderCheck(ctx)
	default:
		err = fmt.Errorf("unknown workflow %q", *workflow)
	}
	if err != nil {
		fatalf("%s failed: %v", *workflow, err)
	}
	fmt.Printf("%s workflow verified\n", *workflow)
}

func newCheckContext() (*checkContext, error) {
	root, err := os.MkdirTemp("", "sync-conformer-workflow-*")
	if err != nil {
		return nil, err
	}
	repository, err := storage.New(root)
	if err != nil {
		os.RemoveAll(root)
		return nil, err
	}
	audio, video, err := media.CalibrationPair()
	if err != nil {
		os.RemoveAll(root)
		return nil, err
	}
	metaAudio, metaVideo, err := metadataPair(audio, video)
	if err != nil {
		os.RemoveAll(root)
		return nil, err
	}
	api := httpapi.New(appservice.New(repository))
	server := httptest.NewServer(api.Handler())
	return &checkContext{
		root:      root,
		server:    server,
		client:    &http.Client{Timeout: 20 * time.Second},
		audio:     audio,
		video:     video,
		metaAudio: metaAudio,
		metaVideo: metaVideo,
	}, nil
}

func runAdmitCheck(ctx *checkContext) error {
	racePayload := func(name string) map[string]any {
		return takePayload("race-take-01", name, ctx.audio, ctx.video)
	}
	statuses := make([]int, 2)
	var wait sync.WaitGroup
	wait.Add(2)
	go func() {
		defer wait.Done()
		response, _, err := ctx.requestJSON(http.MethodPost, "/v1/takes", racePayload("camera left"))
		if err != nil {
			statuses[0] = 0
			return
		}
		statuses[0] = response.StatusCode
	}()
	go func() {
		defer wait.Done()
		response, _, err := ctx.requestJSON(http.MethodPost, "/v1/takes", racePayload("camera right"))
		if err != nil {
			statuses[1] = 0
			return
		}
		statuses[1] = response.StatusCode
	}()
	wait.Wait()
	sort.Ints(statuses)
	if statuses[0] != http.StatusCreated || statuses[1] != http.StatusConflict {
		return fmt.Errorf("identifier race returned statuses %v", statuses)
	}

	response, body, err := ctx.requestJSON(
		http.MethodPost,
		"/v1/takes",
		takePayload("stable-take-01", "stable take", ctx.audio, ctx.video),
	)
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusCreated {
		return fmt.Errorf("first admission returned %d: %s", response.StatusCode, body)
	}
	response, body, err = ctx.requestJSON(
		http.MethodPost,
		"/v1/takes",
		takePayload("stable-take-01", "stable take", ctx.audio, ctx.video),
	)
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("identical retry returned %d: %s", response.StatusCode, body)
	}

	response, body, err = ctx.requestJSON(http.MethodGet, "/v1/takes/stable-take-01", nil)
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("take read returned %d: %s", response.StatusCode, body)
	}
	var admitted domain.MediaTake
	if err := json.Unmarshal(body, &admitted); err != nil {
		return fmt.Errorf("decode admitted take: %w", err)
	}
	if admitted.Digest == "" || admitted.Video.FrameCount != media.CalibrationFrames {
		return fmt.Errorf("admitted take descriptor is incomplete")
	}

	badAudio := base64.StdEncoding.EncodeToString([]byte("not a wave payload"))
	badPayload := takePayload("bad-audio-01", "bad audio", ctx.audio, ctx.video)
	badPayload["audio_base64"] = badAudio
	response, body, err = ctx.requestJSON(http.MethodPost, "/v1/takes", badPayload)
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusUnprocessableEntity {
		return fmt.Errorf("malformed audio returned %d: %s", response.StatusCode, body)
	}
	if code := errorCode(body); code != "invalid_audio" {
		return fmt.Errorf("malformed audio returned code %q", code)
	}

	// Ordinary attached metadata must not be rejected: the metadata pair
	// carries LIST/INFO and private chunks, a sector-style trailing pad, and
	// Y4M X-tags around the same calibration media.
	response, body, err = ctx.requestJSON(
		http.MethodPost,
		"/v1/takes",
		takePayload("meta-take-01", "metadata take", ctx.metaAudio, ctx.metaVideo),
	)
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusCreated {
		return fmt.Errorf("metadata take returned %d: %s", response.StatusCode, body)
	}
	var metaTake domain.MediaTake
	if err := json.Unmarshal(body, &metaTake); err != nil {
		return fmt.Errorf("decode metadata take: %w", err)
	}
	if metaTake.Audio.FrameCount != media.CalibrationSampleRate ||
		metaTake.Video.FrameCount != media.CalibrationFrames {
		return fmt.Errorf("metadata take descriptors do not match the embedded media")
	}

	// Format-legal shells whose structure dwarfs their media must be
	// rejected at admission and must leave no usable record behind.
	junkAudio, junkVideo, err := structuralShells(ctx.audio, ctx.video)
	if err != nil {
		return err
	}
	junkCases := []struct {
		id    string
		audio []byte
		video []byte
		code  string
	}{
		{"junk-chunks-01", junkAudio["chunk-storm"], ctx.video, "invalid_audio"},
		{"junk-meta-01", junkAudio["fat-metadata"], ctx.video, "invalid_audio"},
		{"junk-trailer-01", junkAudio["fat-trailer"], ctx.video, "invalid_audio"},
		{"junk-y4m-head-01", ctx.audio, junkVideo["fat-stream-header"], "invalid_video"},
		{"junk-y4m-frame-01", ctx.audio, junkVideo["fat-frame-header"], "invalid_video"},
	}
	for _, junk := range junkCases {
		response, body, err = ctx.requestJSON(
			http.MethodPost,
			"/v1/takes",
			takePayload(junk.id, "structural shell", junk.audio, junk.video),
		)
		if err != nil {
			return err
		}
		if response.StatusCode != http.StatusUnprocessableEntity {
			return fmt.Errorf("%s returned %d: %s", junk.id, response.StatusCode, body)
		}
		if code := errorCode(body); code != junk.code {
			return fmt.Errorf("%s returned code %q, want %q", junk.id, code, junk.code)
		}
		response, body, err = ctx.requestJSON(http.MethodGet, "/v1/takes/"+junk.id, nil)
		if err != nil {
			return err
		}
		if response.StatusCode != http.StatusNotFound {
			return fmt.Errorf("%s is readable after rejection: %d %s", junk.id, response.StatusCode, body)
		}
	}

	// A failed admission holds no identifier: the same id with valid media
	// must be admitted rather than conflicting with a phantom record.
	response, body, err = ctx.requestJSON(
		http.MethodPost,
		"/v1/takes",
		takePayload("junk-chunks-01", "recovered take", ctx.audio, ctx.video),
	)
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusCreated {
		return fmt.Errorf("identifier reuse after rejection returned %d: %s", response.StatusCode, body)
	}

	persisted, err := storage.New(ctx.root)
	if err != nil {
		return err
	}
	if _, err := persisted.GetTake("stable-take-01"); err != nil {
		return fmt.Errorf("persisted take is unreadable: %w", err)
	}
	for _, junk := range junkCases[1:] {
		if _, err := persisted.GetTake(junk.id); !domain.IsErrorCode(err, "take_not_found") {
			return fmt.Errorf("rejected take %s left a persisted record", junk.id)
		}
	}
	return nil
}

func runAnalyzeCheck(ctx *checkContext) error {
	if err := admitTake(ctx, "analysis-take-01"); err != nil {
		return err
	}
	response, body, err := ctx.requestJSON(
		http.MethodPost,
		"/v1/takes/analysis-take-01/sync-analyses",
		map[string]any{
			"analysis_id":    "analysis-check-01",
			"min_confidence": 0.55,
			"max_offset_ms":  1000,
		},
	)
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusCreated {
		return fmt.Errorf("analysis returned %d: %s", response.StatusCode, body)
	}
	var analysis domain.SyncAnalysis
	if err := json.Unmarshal(body, &analysis); err != nil {
		return fmt.Errorf("decode analysis: %w", err)
	}
	if analysis.State != domain.AnalysisStateReady {
		return fmt.Errorf("analysis state is %s: %s", analysis.State, analysis.ErrorMessage)
	}
	if analysis.OffsetMS == nil || *analysis.OffsetMS != 125 {
		return fmt.Errorf("analysis offset is %v, want 125", analysis.OffsetMS)
	}
	if analysis.AudioEventMS == nil || *analysis.AudioEventMS != 500 {
		return fmt.Errorf("audio event is %v, want 500", analysis.AudioEventMS)
	}
	if analysis.VideoEventMS == nil || *analysis.VideoEventMS != 375 {
		return fmt.Errorf("picture event is %v, want 375", analysis.VideoEventMS)
	}
	if analysis.Confidence < 0.55 {
		return fmt.Errorf("analysis confidence is %.3f", analysis.Confidence)
	}
	persisted, err := storage.New(ctx.root)
	if err != nil {
		return err
	}
	stored, err := persisted.GetAnalysis("analysis-check-01")
	if err != nil {
		return fmt.Errorf("read persisted analysis: %w", err)
	}
	if stored.State != domain.AnalysisStateReady || stored.OffsetMS == nil {
		return fmt.Errorf("persisted analysis is not terminal")
	}

	// The same measurement must hold for media admitted under ordinary
	// metadata wrappers: detection runs on the real content, not the shell.
	if err := admitTakeMedia(ctx, "analysis-take-meta", ctx.metaAudio, ctx.metaVideo); err != nil {
		return err
	}
	response, body, err = ctx.requestJSON(
		http.MethodPost,
		"/v1/takes/analysis-take-meta/sync-analyses",
		map[string]any{
			"analysis_id":    "analysis-meta-01",
			"min_confidence": 0.55,
			"max_offset_ms":  1000,
		},
	)
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusCreated {
		return fmt.Errorf("metadata analysis returned %d: %s", response.StatusCode, body)
	}
	var metaAnalysis domain.SyncAnalysis
	if err := json.Unmarshal(body, &metaAnalysis); err != nil {
		return fmt.Errorf("decode metadata analysis: %w", err)
	}
	if metaAnalysis.State != domain.AnalysisStateReady {
		return fmt.Errorf("metadata analysis state is %s: %s", metaAnalysis.State, metaAnalysis.ErrorMessage)
	}
	if metaAnalysis.OffsetMS == nil || *metaAnalysis.OffsetMS != 125 {
		return fmt.Errorf("metadata analysis offset is %v, want 125", metaAnalysis.OffsetMS)
	}
	if metaAnalysis.AudioEventMS == nil || *metaAnalysis.AudioEventMS != 500 {
		return fmt.Errorf("metadata audio event is %v, want 500", metaAnalysis.AudioEventMS)
	}
	if metaAnalysis.VideoEventMS == nil || *metaAnalysis.VideoEventMS != 375 {
		return fmt.Errorf("metadata picture event is %v, want 375", metaAnalysis.VideoEventMS)
	}
	return nil
}

func runRenderCheck(ctx *checkContext) error {
	if err := admitTakeMedia(ctx, "conform-take-01", ctx.metaAudio, ctx.metaVideo); err != nil {
		return err
	}
	if err := createAnalysis(ctx, "conform-take-01", "conform-analysis-01"); err != nil {
		return err
	}
	response, body, err := ctx.requestJSON(
		http.MethodPost,
		"/v1/sync-analyses/conform-analysis-01/conforms",
		map[string]any{
			"conform_id":        "review-conform-01",
			"max_correction_ms": 250,
		},
	)
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusCreated {
		return fmt.Errorf("conform returned %d: %s", response.StatusCode, body)
	}
	var conform domain.ConformJob
	if err := json.Unmarshal(body, &conform); err != nil {
		return fmt.Errorf("decode conform: %w", err)
	}
	if conform.State != domain.ConformStateCompleted || conform.BundleDigest == "" {
		return fmt.Errorf("conform did not complete: %s", conform.ErrorMessage)
	}

	response, bundle, err := ctx.requestJSON(
		http.MethodGet,
		"/v1/conforms/review-conform-01/bundle",
		nil,
	)
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("bundle read returned %d: %s", response.StatusCode, bundle)
	}
	entries, err := readZip(bundle)
	if err != nil {
		return err
	}
	for _, name := range []string{"audio-aligned.wav", "video-conformed.y4m", "conform-receipt.json"} {
		if len(entries[name]) == 0 {
			return fmt.Errorf("bundle entry %s is missing", name)
		}
	}
	measurement, err := media.AnalyzeSync(entries["audio-aligned.wav"], entries["video-conformed.y4m"])
	if err != nil {
		return fmt.Errorf("analyze aligned bundle: %w", err)
	}
	if measurement.OffsetMS < -2 || measurement.OffsetMS > 2 {
		return fmt.Errorf("aligned audio offset is %d ms", measurement.OffsetMS)
	}
	var receipt domain.ConformReceipt
	if err := json.Unmarshal(entries["conform-receipt.json"], &receipt); err != nil {
		return fmt.Errorf("decode receipt: %w", err)
	}
	if receipt.AppliedOffsetMS != 125 || receipt.InputTakeDigest == "" {
		return fmt.Errorf("receipt does not describe the applied correction")
	}

	response, body, err = ctx.requestJSON(
		http.MethodPost,
		"/v1/sync-analyses/conform-analysis-01/conforms",
		map[string]any{
			"conform_id":        "too-large-01",
			"max_correction_ms": 50,
		},
	)
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusUnprocessableEntity {
		return fmt.Errorf("oversized correction returned %d: %s", response.StatusCode, body)
	}
	response, body, err = ctx.requestJSON(http.MethodGet, "/v1/conforms/too-large-01/bundle", nil)
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusConflict {
		return fmt.Errorf("failed correction exposed a bundle: %d %s", response.StatusCode, body)
	}
	return nil
}

func admitTake(ctx *checkContext, id string) error {
	return admitTakeMedia(ctx, id, ctx.audio, ctx.video)
}

func admitTakeMedia(ctx *checkContext, id string, audio, video []byte) error {
	response, body, err := ctx.requestJSON(
		http.MethodPost,
		"/v1/takes",
		takePayload(id, "workflow take", audio, video),
	)
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusCreated {
		return fmt.Errorf("admit %s returned %d: %s", id, response.StatusCode, body)
	}
	return nil
}

func createAnalysis(ctx *checkContext, takeID, analysisID string) error {
	response, body, err := ctx.requestJSON(
		http.MethodPost,
		"/v1/takes/"+takeID+"/sync-analyses",
		map[string]any{
			"analysis_id":    analysisID,
			"min_confidence": 0.55,
			"max_offset_ms":  1000,
		},
	)
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusCreated {
		return fmt.Errorf("analysis %s returned %d: %s", analysisID, response.StatusCode, body)
	}
	var analysis domain.SyncAnalysis
	if err := json.Unmarshal(body, &analysis); err != nil {
		return err
	}
	if analysis.State != domain.AnalysisStateReady {
		return fmt.Errorf("analysis %s ended in %s", analysisID, analysis.State)
	}
	return nil
}

func takePayload(id, name string, audio, video []byte) map[string]any {
	return map[string]any{
		"take_id":      id,
		"name":         name,
		"note":         "workflow calibration",
		"audio_base64": base64.StdEncoding.EncodeToString(audio),
		"video_base64": base64.StdEncoding.EncodeToString(video),
	}
}

// metadataPair wraps the calibration media in ordinary attached metadata:
// LIST/INFO and private WAV chunks plus a sector-style trailing pad, and
// Y4M X-tags on the stream header and every frame header. The audible and
// visible content is unchanged, so admission and detection must behave
// exactly as they do for the plain pair.
func metadataPair(audio, video []byte) ([]byte, []byte, error) {
	fmtBody, pcm, err := wavParts(audio)
	if err != nil {
		return nil, nil, err
	}
	infoBody := append([]byte("INFO"), riffChunk("ISFT", []byte("sync-calibrator"))...)
	metaAudio := buildWAVPayload([][]byte{
		riffChunk("fmt ", fmtBody),
		riffChunk("LIST", infoBody),
		riffChunk("note", []byte("dual system sound")),
		riffChunk("data", pcm),
	}, make([]byte, 128))

	info, frames, err := calibrationY4MParts(video)
	if err != nil {
		return nil, nil, err
	}
	frameHeaders := make([]string, len(frames))
	for index := range frameHeaders {
		frameHeaders[index] = "FRAME XCOLORSPACE=420JPEG"
	}
	metaVideo := buildY4MPayload(y4mStreamHeader(info, "XCOLORSPACE=420JPEG"), frameHeaders, frames)
	return metaAudio, metaVideo, nil
}

// structuralShells builds format-legal payloads whose structural content
// dwarfs the actual media. Each variant trips exactly one probe budget.
func structuralShells(audio, video []byte) (map[string][]byte, map[string][]byte, error) {
	fmtBody, pcm, err := wavParts(audio)
	if err != nil {
		return nil, nil, err
	}
	info, frames, err := calibrationY4MParts(video)
	if err != nil {
		return nil, nil, err
	}

	stormChunks := [][]byte{riffChunk("fmt ", fmtBody)}
	for index := 0; index < 200; index++ {
		stormChunks = append(stormChunks, riffChunk("JUNK", nil))
	}
	stormChunks = append(stormChunks, riffChunk("data", pcm))
	junkAudio := map[string][]byte{
		"chunk-storm": buildWAVPayload(stormChunks, nil),
		"fat-metadata": buildWAVPayload([][]byte{
			riffChunk("fmt ", fmtBody),
			riffChunk("JUNK", make([]byte, 300<<10)),
			riffChunk("data", pcm),
		}, nil),
		"fat-trailer": buildWAVPayload([][]byte{
			riffChunk("fmt ", fmtBody),
			riffChunk("data", pcm),
		}, make([]byte, 300<<10)),
	}

	plainHeaders := make([]string, len(frames))
	for index := range plainHeaders {
		plainHeaders[index] = "FRAME"
	}
	fatFrameHeaders := make([]string, len(frames))
	copy(fatFrameHeaders, plainHeaders)
	fatFrameHeaders[0] = "FRAME X" + strings.Repeat("t", 300)
	junkVideo := map[string][]byte{
		"fat-stream-header": buildY4MPayload(
			y4mStreamHeader(info, "X"+strings.Repeat("a", 2<<10)),
			plainHeaders,
			frames,
		),
		"fat-frame-header": buildY4MPayload(y4mStreamHeader(info), fatFrameHeaders, frames),
	}
	return junkAudio, junkVideo, nil
}

// wavParts splits a canonical EncodePCM16 payload into its fmt chunk body
// and PCM data body so the check can reassemble custom RIFF containers.
func wavParts(audio []byte) ([]byte, []byte, error) {
	if len(audio) < 44 ||
		string(audio[0:4]) != "RIFF" ||
		string(audio[12:16]) != "fmt " ||
		string(audio[36:40]) != "data" {
		return nil, nil, fmt.Errorf("calibration audio layout is not canonical")
	}
	return audio[20:36], audio[44:], nil
}

func calibrationY4MParts(video []byte) (domain.VideoInfo, [][]byte, error) {
	clip, err := media.InspectVideo(video)
	if err != nil {
		return domain.VideoInfo{}, nil, fmt.Errorf("probe calibration video: %w", err)
	}
	return clip.Info, clip.Frames, nil
}

func riffChunk(id string, body []byte) []byte {
	chunk := make([]byte, 8+len(body)+(len(body)&1))
	copy(chunk[0:4], id)
	binary.LittleEndian.PutUint32(chunk[4:8], uint32(len(body)))
	copy(chunk[8:], body)
	return chunk
}

func buildWAVPayload(chunks [][]byte, trailing []byte) []byte {
	var body bytes.Buffer
	body.WriteString("WAVE")
	for _, chunk := range chunks {
		body.Write(chunk)
	}
	payload := body.Bytes()
	out := make([]byte, 0, 8+len(payload)+len(trailing))
	out = append(out, []byte("RIFF")...)
	var size [4]byte
	binary.LittleEndian.PutUint32(size[0:4], uint32(len(payload)))
	out = append(out, size[:]...)
	out = append(out, payload...)
	return append(out, trailing...)
}

func y4mStreamHeader(info domain.VideoInfo, extraTags ...string) string {
	header := fmt.Sprintf(
		"YUV4MPEG2 W%d H%d F%d:%d Ip A0:0 %s",
		info.Width,
		info.Height,
		info.FPSNum,
		info.FPSDen,
		info.Chroma,
	)
	for _, tag := range extraTags {
		header += " " + tag
	}
	return header
}

func buildY4MPayload(streamHeader string, frameHeaders []string, frames [][]byte) []byte {
	var out bytes.Buffer
	out.WriteString(streamHeader)
	out.WriteByte('\n')
	for index, frame := range frames {
		out.WriteString(frameHeaders[index])
		out.WriteByte('\n')
		out.Write(frame)
	}
	return out.Bytes()
}

func (ctx *checkContext) requestJSON(
	method string,
	path string,
	payload any,
) (*http.Response, []byte, error) {
	var body io.Reader
	if payload != nil {
		data, err := json.Marshal(payload)
		if err != nil {
			return nil, nil, err
		}
		body = bytes.NewReader(data)
	}
	request, err := http.NewRequest(method, ctx.server.URL+path, body)
	if err != nil {
		return nil, nil, err
	}
	if payload != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := ctx.client.Do(request)
	if err != nil {
		return nil, nil, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, nil, err
	}
	return response, data, nil
}

func readZip(data []byte) (map[string][]byte, error) {
	reader, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, fmt.Errorf("open bundle: %w", err)
	}
	entries := make(map[string][]byte)
	for _, entry := range reader.File {
		if strings.Contains(entry.Name, "..") {
			return nil, fmt.Errorf("bundle entry path is unsafe")
		}
		file, err := entry.Open()
		if err != nil {
			return nil, err
		}
		content, err := io.ReadAll(file)
		file.Close()
		if err != nil {
			return nil, err
		}
		entries[entry.Name] = content
	}
	return entries, nil
}

func errorCode(body []byte) string {
	var envelope struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return ""
	}
	return envelope.Error.Code
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
