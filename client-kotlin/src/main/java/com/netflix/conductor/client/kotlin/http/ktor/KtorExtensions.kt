import io.ktor.client.call.*
import io.ktor.client.statement.*
import io.ktor.http.*

suspend inline fun <reified T> HttpResponse.bodySafe(): List<T> {
    if (call.response.status == HttpStatusCode.NoContent) return emptyList()
    return body()
}