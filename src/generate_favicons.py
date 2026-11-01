"""Generate solid-color SVG, PNG, and ICO favicons using Python standard library."""

import struct
import zlib
from pathlib import Path


def make_png_bytes(width: int, height: int) -> bytes:
    """Create raw RGBA PNG bytes without external dependencies."""
    raw = bytearray()
    for y in range(height):
        raw.append(0)  # filter byte: none
        for x in range(width):
            # Flat solid palette: dark navy #1e293b, accent blue #2563eb, slate #64748b
            if (6 <= x <= 13 and 6 <= y <= 13) or (18 <= x <= 25 and 18 <= y <= 25):
                raw.extend((37, 99, 235, 255))
            elif (18 <= x <= 25 and 6 <= y <= 13) or (6 <= x <= 13 and 18 <= y <= 25):
                raw.extend((100, 116, 139, 255))
            else:
                raw.extend((30, 41, 59, 255))

    def chunk(tag: bytes, data: bytes) -> bytes:
        crc = zlib.crc32(tag + data) & 0xFFFFFFFF
        return struct.pack(">I", len(data)) + tag + data + struct.pack(">I", crc)

    sig = b"\x89PNG\r\n\x1a\n"
    ihdr = chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 6, 0, 0, 0))
    idat = chunk(b"IDAT", zlib.compress(bytes(raw), 9))
    iend = chunk(b"IEND", b"")
    return sig + ihdr + idat + iend


def generate_favicons() -> None:
    static_dir = Path("static")
    static_dir.mkdir(parents=True, exist_ok=True)

    # 1. Favicon SVG
    svg_code = (
        '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32" width="32" height="32">\n'
        '  <rect width="32" height="32" fill="#1e293b" rx="2" ry="2"/>\n'
        '  <rect x="6" y="6" width="8" height="8" fill="#2563eb"/>\n'
        '  <rect x="18" y="6" width="8" height="8" fill="#64748b"/>\n'
        '  <rect x="6" y="18" width="8" height="8" fill="#64748b"/>\n'
        '  <rect x="18" y="18" width="8" height="8" fill="#2563eb"/>\n'
        "</svg>\n"
    )
    with open(static_dir / "favicon.svg", "w", encoding="utf-8") as f:
        f.write(svg_code)

    # 2. Favicon 32x32 PNG
    png_bytes = make_png_bytes(32, 32)
    with open(static_dir / "favicon-32x32.png", "wb") as f:
        f.write(png_bytes)

    # 3. Favicon ICO containing PNG payload
    ico_header = struct.pack("<HHH", 0, 1, 1)
    ico_entry = struct.pack("<BBBBHHII", 32, 32, 0, 0, 1, 32, len(png_bytes), 22)
    ico_bytes = ico_header + ico_entry + png_bytes
    with open(static_dir / "favicon.ico", "wb") as f:
        f.write(ico_bytes)

    print("Generated favicon.svg, favicon-32x32.png, and favicon.ico using standard library.")


if __name__ == "__main__":
    generate_favicons()
