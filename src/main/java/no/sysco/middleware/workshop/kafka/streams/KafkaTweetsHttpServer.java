package no.sysco.middleware.workshop.kafka.streams;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.http.javadsl.ConnectHttp;
import akka.http.javadsl.Http;
import akka.http.javadsl.ServerBinding;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.server.AllDirectives;
import akka.http.javadsl.server.PathMatchers;
import akka.http.javadsl.server.Route;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Flow;

import java.util.concurrent.CompletionStage;

public class KafkaTweetsHttpServer extends AllDirectives {

  final KafkaTweetsStreams streams;

  private KafkaTweetsHttpServer(KafkaTweetsStreams streams) {
    this.streams = streams;
  }

  public static void main(String[] args) throws Exception {
    ActorSystem system = ActorSystem.create("kafka-tweets-stream");
    ActorMaterializer materializer = ActorMaterializer.create(system);

    final Http http = Http.get(system);

    KafkaTweetsStreams streams = new KafkaTweetsStreams();
    streams.start();
    KafkaTweetsHttpServer server = new KafkaTweetsHttpServer(streams);

    Flow<HttpRequest, HttpResponse, NotUsed> routeFlow = server.createRoute().flow(system, materializer);
    CompletionStage<ServerBinding> binding = http.bindAndHandle(routeFlow, ConnectHttp.toHost("0.0.0.0", 8080), materializer);

    System.out.println("Server online at http://localhost:8080/\nPress RETURN to stop...");
    System.in.read(); // let it run until user presses return

    binding
        .thenCompose(ServerBinding::unbind) // trigger unbinding from the port
        .thenAccept(unbound -> {
          streams.stop();
          system.terminate();
        }); // and shutdown when done
  }

  private Route createRoute() {
    return route(
        get(() -> path("hello", () -> complete("world"))),
        get(() -> path("hashtags", () -> complete(streams.getHashtags()))),
        get(() -> (pathPrefix(PathMatchers.segment("hashtag-progress").slash(PathMatchers.segment()),
            hashtag -> complete(streams.getHashtagProgress(hashtag))))));
  }
}
