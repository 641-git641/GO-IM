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
	_ "golang.org/x/image/webp" // 注册 webp 解码器
)

// ThumbnailMaxDim 是缩略图的最大宽度或高度(像素)。
const ThumbnailMaxDim = 200

// Thumbnail 为图片生成缩略图,保持宽高比。
// 返回缩略图的 JPEG 字节;如果输入不是受支持的图片格式或解码失败,
// 则返回错误。输出在保持宽高比的同时,
// 不超过 ThumbnailMaxDim×ThumbnailMaxDim 的边界框。
func Thumbnail(data []byte) ([]byte, error) {
	// 解码源图片。我们忽略格式字符串,
	// 因为无论源格式如何,缩略图始终编码为 JPEG。
	src, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}

	// 计算保持宽高比的缩略图尺寸。
	srcBounds := src.Bounds()
	srcW := srcBounds.Dx()
	srcH := srcBounds.Dy()

	if srcW <= 0 || srcH <= 0 {
		return nil, fmt.Errorf("invalid image dimensions: %dx%d", srcW, srcH)
	}

	// 如果已小于最大值,则保持原始尺寸。
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

	// 创建目标图片并使用 CatmullRom 缩放绘制。
	dst := image.NewRGBA(image.Rect(0, 0, dstW, dstH))
	draw.CatmullRom.Scale(dst, dst.Bounds(), src, srcBounds, draw.Over, nil)

	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, dst, &jpeg.Options{Quality: 80}); err != nil {
		return nil, fmt.Errorf("encode jpeg: %w", err)
	}

	return buf.Bytes(), nil
}

// ImageDimensions 在不解码图片的情况下返回其宽度和高度。
// 如果数据不是可解码的图片,则返回 (0, 0)。
func ImageDimensions(data []byte) (width, height int) {
	cfg, _, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil {
		return 0, 0
	}
	return cfg.Width, cfg.Height
}

// IsImageMIME 如果 MIME 类型表示图片则返回 true。
func IsImageMIME(mime string) bool {
	return strings.HasPrefix(mime, "image/") &&
		mime != "image/svg+xml" // SVG 是矢量图 —— 不生成缩略图
}

// 通过导入确保解码器可用。
var _ = gif.Decode       // gif 解码器
var _ = png.Decode       // png 解码器
var _ = jpeg.Decode      // jpeg 解码器
