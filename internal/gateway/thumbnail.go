package gateway

import (
	"bytes"
	"fmt"
	"image"
	"image/gif"
	"image/jpeg"
	"image/png"
	"strings"

	"golang.org/x/image/draw"
	_ "golang.org/x/image/webp" // register webp decoder
)

// ThumbnailMaxDim is the maximum width or height of a thumbnail in pixels.
const ThumbnailMaxDim = 200

// Thumbnail generates a thumbnail for an image, preserving aspect ratio.
// Returns the thumbnail JPEG bytes, or an error if the input is not a supported
// image format or decoding fails. The output fits within a ThumbnailMaxDim×ThumbnailMaxDim
// bounding box while preserving aspect ratio.
func Thumbnail(data []byte) ([]byte, error) {
	// Decode the source image. We ignore the format string
	// because the thumbnail is always encoded as JPEG regardless
	// of the source format.
	src, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}

	// Compute thumbnail dimensions preserving aspect ratio.
	srcBounds := src.Bounds()
	srcW := srcBounds.Dx()
	srcH := srcBounds.Dy()

	if srcW <= 0 || srcH <= 0 {
		return nil, fmt.Errorf("invalid image dimensions: %dx%d", srcW, srcH)
	}

	// If already smaller than max, keep original size.
	dstW, dstH := srcW, srcH
	if dstW > ThumbnailMaxDim || dstH > ThumbnailMaxDim {
		scale := float64(ThumbnailMaxDim) / float64(max(srcW, srcH))
		dstW = int(float64(srcW) * scale)
		dstH = int(float64(srcH) * scale)
		if dstW < 1 {
			dstW = 1
		}
		if dstH < 1 {
			dstH = 1
		}
	}

	// Create destination image and draw with CatmullRom scaling.
	dst := image.NewRGBA(image.Rect(0, 0, dstW, dstH))
	draw.CatmullRom.Scale(dst, dst.Bounds(), src, srcBounds, draw.Over, nil)

	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, dst, &jpeg.Options{Quality: 80}); err != nil {
		return nil, fmt.Errorf("encode jpeg: %w", err)
	}

	return buf.Bytes(), nil
}

// ImageDimensions returns the width and height of an image without resizing.
// Returns (0, 0) if the data is not a decodable image.
func ImageDimensions(data []byte) (width, height int) {
	cfg, _, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil {
		return 0, 0
	}
	return cfg.Width, cfg.Height
}

// IsImageMIME returns true if the MIME type indicates an image.
func IsImageMIME(mime string) bool {
	return strings.HasPrefix(mime, "image/") &&
		mime != "image/svg+xml" // SVG is vector — no thumbnail
}

// Ensure decoders are available by importing them.
var _ = gif.Decode       // gif decoder
var _ = png.Decode       // png decoder
var _ = jpeg.Decode      // jpeg decoder
