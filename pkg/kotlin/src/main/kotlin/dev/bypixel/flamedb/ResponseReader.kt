package dev.bypixel.flamedb

import java.io.EOFException
import java.io.IOException
import java.io.InputStream
import java.nio.ByteBuffer
import java.nio.charset.CharacterCodingException
import java.nio.charset.CodingErrorAction

internal class ProtocolResponseException(val kind: FlameDBErrorKind, message: String, cause: Throwable? = null) :
    IOException(message, cause)

/** Counts wire bytes before LF (including optional CR), never replacement-decodes UTF-8. */
internal class ResponseReader(private val input: InputStream, private val limit: Int) {
    private val buffer = ByteArray(minOf(8192, limit.toLong() + 1L).toInt())
    private var position = 0
    private var end = 0

    fun readLine(beforeRead: () -> Unit): String {
        var bytes = ByteArray(minOf(256, limit))
        var size = 0
        while (true) {
            if (position == end) {
                beforeRead()
                end = input.read(buffer)
                position = 0
                if (end < 0) throw ProtocolResponseException(FlameDBErrorKind.INVALID_RESPONSE,
                    "incomplete FlameDB response: missing LF", EOFException("connection closed before response terminator"))
                if (end == 0) continue
            }
            val byte = buffer[position++]
            if (byte == '\n'.code.toByte()) break
            if (size == limit) throw ProtocolResponseException(FlameDBErrorKind.RESPONSE_TOO_LARGE,
                "FlameDB response exceeds maxResponseBytes=$limit")
            if (size == bytes.size) {
                bytes = bytes.copyOf(minOf(limit.toLong(), bytes.size.toLong() * 2L).toInt())
            }
            bytes[size++] = byte
        }
        if (size > 0 && bytes[size - 1] == '\r'.code.toByte()) size--
        return try {
            Charsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes, 0, size)).toString()
        } catch (error: CharacterCodingException) {
            throw ProtocolResponseException(FlameDBErrorKind.INVALID_RESPONSE, "invalid UTF-8 in FlameDB response", error)
        }
    }
}
