package store

import (
	"testing"
)

var sniffTests = []struct {
	desc        string
	data        []byte
	contentType string
	result      bool
}{
	// Audio types.
	{"MIDI audio", []byte("MThd\x00\x00\x00\x06\x00\x01"), "audio/midi", true},
	{"MP3 audio/MPEG audio", []byte("ID3\x03\x00\x00\x00\x00\x0f"), "audio/mpeg", false},
	{"WAV audio #1", []byte("RIFFb\xb8\x00\x00WAVEfmt \x12\x00\x00\x00\x06"), "audio/wave", false},
	{"WAV audio #2", []byte("RIFF,\x00\x00\x00WAVEfmt \x12\x00\x00\x00\x06"), "audio/wave", false},
	{"AIFF audio #1", []byte("FORM\x00\x00\x00\x00AIFFCOMM\x00\x00\x00\x12\x00\x01\x00\x00\x57\x55\x00\x10\x40\x0d\xf3\x34"), "audio/aiff", true},
	{"OGG audio", []byte("OggS\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x7e\x46\x00\x00\x00\x00\x00\x00\x1f\xf6\xb4\xfc\x01\x1e\x01\x76\x6f\x72"), "application/ogg", true},
	{"MP3 audio/MPEG audio #2", []byte("ID3\x03\x00\x00\x00\x00\x04"), "audio/mpeg", false},
	{"MP3 audio/MPEG audio #3", []byte("ID3\x03\x00\x00\x00\x00\x0a"), "audio/mpeg", false},
}

func ValueWithKind(header []byte, size int) []byte {
	b := make([]byte, size)
	offsetHeader := len(header)
	for i := 0; i < offsetHeader; i++ {
		b[i] = byte(header[i])
	}
	return b
}

func TestCompress(t *testing.T) {
	Conf.InitDefault()
	Conf.NotCompress = map[string]bool{
		"audio/mpeg": true,
		"audio/wave": true,
	}
	t.Log(Conf.NotCompress)

	for _, st := range sniffTests {

		value := ValueWithKind(st.data, 512)
		header := value[:512]
		if result := NeedCompress(header); result != st.result {
			t.Errorf("type %s compress is error, except %v , but got %v", st.contentType, st.result, result)
		}

	}
}
