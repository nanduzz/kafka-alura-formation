import java.io.File
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption

class IO {
    companion object {
        fun copyTo(source: File, target: File) {
            target.parentFile.mkdirs()
            Files.copy(source.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING)
        }

        fun append(target: File, content: String) {
            Files.write(target.toPath(), content.toByteArray(), StandardOpenOption.APPEND)
        }
    }
}
