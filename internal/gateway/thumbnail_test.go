package gateway

import (
	"bytes"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"testing"
)

// newTestJPEG 在内存中创建一张小型 JPEG 图片。
func newTestJPEG(w, h int) []byte {
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	// 用渐变图案填充。
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			img.Set(x, y, color.RGBA{
				R: uint8(x * 255 / w),
				G: uint8(y * 255 / h),
				B: 128,
				A: 255,
			})
		}
	}
	var buf bytes.Buffer
	jpeg.Encode(&buf, img, &jpeg.Options{Quality: 90})
	return buf.Bytes()
}

// newTestPNG 在内存中创建一张小型 PNG 图片。
func newTestPNG(w, h int) []byte {
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			img.Set(x, y, color.RGBA{
				R: uint8(x * 255 / w),
				G: uint8(y * 255 / h),
				B: 128,
				A: 255,
			})
		}
	}
	var buf bytes.Buffer
	png.Encode(&buf, img)
	return buf.Bytes()
}

func TestThumbnailJPEG(t *testing.T) {
	orig := newTestJPEG(800, 600)
	thumb, err := Thumbnail(orig)
	if err != nil {
		t.Fatalf("Thumbnail: %v", err)
	}

	// 解码缩略图以检查尺寸。
	img, _, err := image.Decode(bytes.NewReader(thumb))
	if err != nil {
		t.Fatalf("decode thumbnail: %v", err)
	}

	b := img.Bounds()
	if b.Dx() > ThumbnailMaxDim || b.Dy() > ThumbnailMaxDim {
		t.Errorf("thumbnail too large: %dx%d, max is %d", b.Dx(), b.Dy(), ThumbnailMaxDim)
	}

	// 应保持宽高比：800:600 = 4:3。
	// 最大边 = 200 → 200×150。
	if b.Dx() != 200 || b.Dy() != 150 {
		t.Errorf("expected 200×150 for 800:600, got %dx%d", b.Dx(), b.Dy())
	}
}

func TestThumbnailPNG(t *testing.T) {
	orig := newTestPNG(400, 300)
	thumb, err := Thumbnail(orig)
	if err != nil {
		t.Fatalf("Thumbnail PNG: %v", err)
	}

	img, _, err := image.Decode(bytes.NewReader(thumb))
	if err != nil {
		t.Fatalf("decode thumbnail: %v", err)
	}

	b := img.Bounds()
	// 400:300 = 4:3。最大 200 → 200×150。
	if b.Dx() != 200 || b.Dy() != 150 {
		t.Errorf("expected 200×150 for 400:300, got %dx%d", b.Dx(), b.Dy())
	}
}

func TestThumbnailSmallImage(t *testing.T) {
	// 图片小于最大值 —— 不应放大。
	orig := newTestJPEG(50, 50)
	thumb, err := Thumbnail(orig)
	if err != nil {
		t.Fatalf("Thumbnail small: %v", err)
	}

	img, _, err := image.Decode(bytes.NewReader(thumb))
	if err != nil {
		t.Fatalf("decode thumbnail: %v", err)
	}

	b := img.Bounds()
	if b.Dx() != 50 || b.Dy() != 50 {
		t.Errorf("expected 50×50 (no upscale), got %dx%d", b.Dx(), b.Dy())
	}
}

func TestThumbnailSquareTall(t *testing.T) {
	// 高图：100×400。最大 200 → 50×200。
	orig := newTestJPEG(100, 400)
	thumb, err := Thumbnail(orig)
	if err != nil {
		t.Fatalf("Thumbnail tall: %v", err)
	}

	img, _, err := image.Decode(bytes.NewReader(thumb))
	if err != nil {
		t.Fatalf("decode thumbnail: %v", err)
	}

	b := img.Bounds()
	if b.Dx() != 50 || b.Dy() != 200 {
		t.Errorf("expected 50×200 for 100:400, got %dx%d", b.Dx(), b.Dy())
	}
}

func TestThumbnailWide(t *testing.T) {
	// 宽图：400×100。最大 200 → 200×50。
	orig := newTestJPEG(400, 100)
	thumb, err := Thumbnail(orig)
	if err != nil {
		t.Fatalf("Thumbnail wide: %v", err)
	}

	img, _, err := image.Decode(bytes.NewReader(thumb))
	if err != nil {
		t.Fatalf("decode thumbnail: %v", err)
	}

	b := img.Bounds()
	if b.Dx() != 200 || b.Dy() != 50 {
		t.Errorf("expected 200×50 for 400:100, got %dx%d", b.Dx(), b.Dy())
	}
}

func TestThumbnailInvalidData(t *testing.T) {
	_, err := Thumbnail([]byte("not an image"))
	if err == nil {
		t.Error("expected error for non-image data")
	}
}

func TestImageDimensions(t *testing.T) {
	data := newTestJPEG(800, 600)
	w, h := ImageDimensions(data)
	if w != 800 {
		t.Errorf("expected width 800, got %d", w)
	}
	if h != 600 {
		t.Errorf("expected height 600, got %d", h)
	}
}

func TestImageDimensionsInvalid(t *testing.T) {
	w, h := ImageDimensions([]byte("not an image"))
	if w != 0 || h != 0 {
		t.Errorf("expected 0,0 for invalid data, got %d,%d", w, h)
	}
}

func TestIsImageMIME(t *testing.T) {
	tests := []struct {
		mime string
		want bool
	}{
		{"image/jpeg", true},
		{"image/png", true},
		{"image/gif", true},
		{"image/webp", true},
		{"image/svg+xml", false}, // SVG 是矢量图，无缩略图
		{"text/plain", false},
		{"application/pdf", false},
		{"video/mp4", false},
	}
	for _, tc := range tests {
		got := IsImageMIME(tc.mime)
		if got != tc.want {
			t.Errorf("IsImageMIME(%q) = %v, want %v", tc.mime, got, tc.want)
		}
	}
}
