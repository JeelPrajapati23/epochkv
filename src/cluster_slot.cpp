#include "cluster_slot.hpp"

#include <array>

namespace cluster {

uint16_t crc16(const char* data, size_t len) {
    // Table-driven, one byte per step: the table holds the CRC of every
    // possible high byte, built once from the bitwise definition.
    static const std::array<uint16_t, 256> table = [] {
        std::array<uint16_t, 256> t{};
        for (uint32_t i = 0; i < 256; ++i) {
            uint16_t crc = static_cast<uint16_t>(i << 8);
            for (int bit = 0; bit < 8; ++bit) {
                crc = static_cast<uint16_t>((crc & 0x8000) ? (crc << 1) ^ 0x1021 : crc << 1);
            }
            t[i] = crc;
        }
        return t;
    }();
    uint16_t crc = 0;
    for (size_t i = 0; i < len; ++i) {
        crc = static_cast<uint16_t>((crc << 8) ^ table[((crc >> 8) ^ static_cast<uint8_t>(data[i])) & 0xFF]);
    }
    return crc;
}

int key_hash_slot(std::string_view key) {
    size_t open = key.find('{');
    if (open != std::string_view::npos) {
        size_t close = key.find('}', open + 1);
        if (close != std::string_view::npos && close != open + 1) {
            key = key.substr(open + 1, close - open - 1);
        }
    }
    // 16384 is a power of two, so the mod is a mask.
    return crc16(key.data(), key.size()) & (kSlots - 1);
}

}  // namespace cluster
