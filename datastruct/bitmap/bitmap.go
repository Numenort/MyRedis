package bitmap

type Bitmap []byte

func New() *Bitmap {
	b := Bitmap(make([]byte, 0))
	return &b
}

// convert bit to bytes
func toBytesSize(bitsize int64) int64 {
	if bitsize%8 == 0 {
		return bitsize / 8
	}
	return bitsize/8 + 1
}

func (b *Bitmap) grow(bitsize int64) {
	byteSize := toBytesSize(bitsize)
	// gap <= 0，即当前的 bitmap可以容纳 bitsize大小
	gap := byteSize - int64(len(*b))
	if gap <= 0 {
		return
	}
	*b = append(*b, make([]byte, gap)...)
}

func (b *Bitmap) Bitsize() int {
	return len(*b) * 8
}

func FromBytes(bytes []byte) *Bitmap {
	bm := Bitmap(bytes)
	return &bm
}

func (b *Bitmap) ToBytes() []byte {
	return *b
}

func (b *Bitmap) SetBit(offset int64, val byte) {
	byteIndex := offset / 8
	bitOffset := offset % 8
	// 00000100 if bitOffset equals 3
	mask := byte(1 << bitOffset)
	b.grow(offset + 1)
	if val > 0 {
		// set bit
		(*b)[byteIndex] |= mask
	} else {
		// clear bit
		(*b)[byteIndex] &^= mask
	}
}

func (b *Bitmap) GetBit(offset int64) byte {
	byteIndex := offset / 8
	bitOffset := offset % 8

	if byteIndex >= int64(len(*b)) {
		return 0
	}
	// 得到 bit 位
	return ((*b)[byteIndex] >> bitOffset) & 0x01
}

// 是否继续遍历
type Callback func(offset int64, val byte) bool

// 遍历从 begin 位置开始到 end 位置中的每个bit
func (b *Bitmap) ForEachBit(begin int64, end int64, cb Callback) {
	offset := begin
	byteIndex := offset / 8
	bitOffset := offset % 8

	for byteIndex < int64(len(*b)) {
		// 起始比特位，获取一个字节的值
		b := (*b)[byteIndex]
		for bitOffset < 8 {
			// 由每个字节，获取每个bit位
			bit := byte(b >> bitOffset & 0x01)
			if !cb(offset, bit) {
				return
			}
			bitOffset++
			offset++
			if offset >= end && end != 0 {
				break
			}
		}
		byteIndex++
		bitOffset = 0
		if end > 0 && offset >= end {
			break
		}
	}
}

// 遍历从 begin 位置开始到 end 位置中的每个byte
func (b *Bitmap) ForEachByte(begin int, end int, cb Callback) {
	if end == 0 {
		end = len(*b)
	} else if end > len(*b) {
		end = len(*b)
	}
	for i := begin; i < end; i++ {
		if !cb(int64(i), (*b)[i]) {
			return
		}
	}
}
