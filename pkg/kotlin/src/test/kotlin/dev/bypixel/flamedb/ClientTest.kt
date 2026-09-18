package dev.bypixel.flamedb
import kotlin.test.*
import kotlinx.coroutines.runBlocking
import java.net.ServerSocket
import kotlin.concurrent.thread
import java.util.concurrent.atomic.AtomicReference
class ClientTest {
 @Test fun resultMappingsAndEscaping() = runBlocking {
  val received = AtomicReference("")
  ServerSocket(0).use { server ->
   val worker=thread(isDaemon=true) {
    server.accept().use { socket ->
     val reader=socket.getInputStream().bufferedReader(Charsets.UTF_8)
     val writer=socket.getOutputStream().bufferedWriter(Charsets.UTF_8)
     fun reply(s:String){writer.write(s);writer.newLine();writer.flush()}
     reply("""{"auth":"required"}""");reader.readLine();reply("""{"auth":"ok"}""")
     while(true){val line=reader.readLine()?:break;received.set(line)
      when {
       line.startsWith("STATS") -> reply("""{"stats":{"metric":"m","tag_stats":[{"tag_key":"p","cardinality":2}]}}""")
       line.startsWith("LEADERBOARD") -> reply("""{"leaderboard":[{"entity_id":"Hello 🌍🔥","value":42}]}""")
       else -> reply("{}")
      }
     }
    }
   }
   FlameDB.connect(FlameDBConfig("127.0.0.1",server.localPort,"test")).use { db ->
    assertEquals(42.0,db.leaderboard("m").single().score)
    assertEquals(2,db.stats("m","p").tagStats.single().cardinality)
    db.write("m",1.0,WriteOptions(tags=mapOf("p" to "a\"\nb")))
    assertTrue(received.get().contains("p=\"a\\\"\\nb\""))
    assertFailsWith<IllegalArgumentException>{db.write("m 1 lb=",1.0)}
   }
   worker.join(1000)
  }
 }
 @Test fun failedAuthClosesSocket()=runBlocking {
  ServerSocket(0).use { server ->
   val worker=thread(isDaemon=true){server.accept().use{socket->
    socket.soTimeout=2000
    val reader=socket.getInputStream().bufferedReader();val writer=socket.getOutputStream().bufferedWriter()
    writer.write("{\"auth\":\"required\"}\n");writer.flush();reader.readLine()
    writer.write("{\"error\":\"invalid key\"}\n");writer.flush();assertEquals(-1,reader.read())
   }}
   assertFailsWith<FlameDBException>{FlameDB.connect(FlameDBConfig("127.0.0.1",server.localPort,"test"))}
   worker.join(3000)
  }
 }
}
