import { useState } from 'react';
import { getFileURL, getFileCategory } from '@/lib/utils';
import { FileText, Image, Film, Music, X, Download, ZoomIn, ZoomOut, RotateCw } from 'lucide-react';

interface FilePreviewProps {
  fileId: string;
  fileName: string;
  mime: string;
  size: number;
  width?: number;
  height?: number;
  caption?: string;
  uid: string;
  token: string;
}

function formatSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

/** 全屏图片灯箱 */
function ImageLightbox({
  src,
  alt,
  fileName,
  onClose,
}: {
  src: string;
  alt: string;
  fileName: string;
  onClose: () => void;
}) {
  const [scale, setScale] = useState(1);
  const [rotation, setRotation] = useState(0);

  const zoomIn = () => setScale((s) => Math.min(s + 0.5, 5));
  const zoomOut = () => setScale((s) => Math.max(s - 0.5, 0.5));
  const rotate = () => setRotation((r) => r + 90);

  return (
    <div
      className="fixed inset-0 z-50 bg-black/90 flex items-center justify-center"
      onClick={onClose}
    >
      {/* 工具栏 */}
      <div className="absolute top-4 right-4 flex items-center gap-2 z-10">
        <button
          onClick={(e) => { e.stopPropagation(); zoomOut(); }}
          className="p-2 rounded-lg bg-white/10 text-white hover:bg-white/20 transition-colors"
          title="缩小"
        >
          <ZoomOut className="w-5 h-5" />
        </button>
        <button
          onClick={(e) => { e.stopPropagation(); zoomIn(); }}
          className="p-2 rounded-lg bg-white/10 text-white hover:bg-white/20 transition-colors"
          title="放大"
        >
          <ZoomIn className="w-5 h-5" />
        </button>
        <button
          onClick={(e) => { e.stopPropagation(); rotate(); }}
          className="p-2 rounded-lg bg-white/10 text-white hover:bg-white/20 transition-colors"
          title="旋转"
        >
          <RotateCw className="w-5 h-5" />
        </button>
        <a
          href={src}
          download={fileName}
          onClick={(e) => e.stopPropagation()}
          className="p-2 rounded-lg bg-white/10 text-white hover:bg-white/20 transition-colors"
          title="下载原图"
        >
          <Download className="w-5 h-5" />
        </a>
        <button
          onClick={onClose}
          className="p-2 rounded-lg bg-white/10 text-white hover:bg-white/20 transition-colors"
          title="关闭"
        >
          <X className="w-5 h-5" />
        </button>
      </div>

      {/* 图片名称 */}
      <div className="absolute top-4 left-4 text-white/70 text-sm z-10">{fileName}</div>

      {/* 图片 */}
      <img
        src={src}
        alt={alt}
        className="max-w-[90vw] max-h-[90vh] object-contain cursor-pointer transition-transform duration-200"
        style={{
          transform: `scale(${scale}) rotate(${rotation}deg)`,
        }}
        onClick={(e) => e.stopPropagation()}
      />
    </div>
  );
}

export default function FilePreview({
  fileId,
  fileName,
  mime,
  size,
  width,
  height,
  caption,
  uid,
  token,
}: FilePreviewProps) {
  const category = getFileCategory(mime);
  const thumbUrl = getFileURL(fileId, uid, token, true);
  const fullUrl = getFileURL(fileId, uid, token);
  const [lightboxOpen, setLightboxOpen] = useState(false);

  if (category === 'image') {
    return (
      <div className="space-y-1 max-w-60">
        <img
          src={thumbUrl}
          alt={fileName}
          className="rounded-lg max-h-48 object-cover cursor-pointer hover:opacity-90 transition-opacity"
          loading="lazy"
          onClick={() => setLightboxOpen(true)}
        />
        {caption && <p className="text-sm opacity-90">{caption}</p>}

        {lightboxOpen && (
          <ImageLightbox
            src={fullUrl}
            alt={fileName}
            fileName={fileName}
            onClose={() => setLightboxOpen(false)}
          />
        )}
      </div>
    );
  }

  if (category === 'video') {
    return (
      <a
        href={fullUrl}
        target="_blank"
        rel="noopener noreferrer"
        className="flex items-center gap-2 p-2 rounded-lg bg-black/10 hover:bg-black/20 transition-colors"
      >
        <Film className="w-6 h-6" />
        <div>
          <p className="text-sm font-medium">{fileName}</p>
          <p className="text-xs opacity-70">{formatSize(size)}</p>
        </div>
      </a>
    );
  }

  if (category === 'audio') {
    return (
      <div className="flex items-center gap-2 p-2 rounded-lg bg-black/10 min-w-[180px]">
        <Music className="w-6 h-6" />
        <div className="flex-1 min-w-0">
          <p className="text-sm font-medium truncate">{fileName}</p>
          <audio controls className="mt-1 h-8 w-full">
            <source src={fullUrl} type={mime} />
          </audio>
        </div>
      </div>
    );
  }

  // 普通文件
  return (
    <a
      href={fullUrl}
      target="_blank"
      rel="noopener noreferrer"
      className="flex items-center gap-2 p-2 rounded-lg bg-black/10 hover:bg-black/20 transition-colors min-w-[200px]"
    >
      <FileText className="w-6 h-6 flex-shrink-0" />
      <div className="min-w-0">
        <p className="text-sm font-medium truncate">{fileName}</p>
        <p className="text-xs opacity-70">{formatSize(size)}</p>
      </div>
    </a>
  );
}
