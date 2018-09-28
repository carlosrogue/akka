/**
 * Copyright (C) 2018 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.akka.cluster.sharding.typed;

import akka.Done;
import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.cluster.sharding.typed.javadsl.EntityRef;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;
import akka.cluster.sharding.typed.javadsl.PersistentEntity;
import akka.cluster.sharding.typed.javadsl.PersistentEntityRegistry;
import akka.cluster.sharding.typed.javadsl.ShardedPersistentEntity;
import akka.persistence.typed.javadsl.CommandHandler;
import akka.persistence.typed.javadsl.CommandHandlerBuilder;
import akka.persistence.typed.javadsl.EventHandler;
import akka.util.Timeout;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

public class PersistentEntityCompileOnlyTest {


  public static class BlogService {
    private final ActorSystem<?> system;
    private final PersistentEntityRegistry registry;
    private final Timeout askTimeout = Timeout.create(Duration.ofSeconds(5));

    // registration at startup
    public BlogService(ActorSystem<?> system) {
      this.system = system;
      registry = PersistentEntityRegistry.get(system);

      registry.start(
          ShardedPersistentEntity.create(
              BlogEntity::new, //i.e. entityId -> new BlogEntity(entityId)
              blogTypeKey,
              new PassivatePost()));
    }

    // usage example
    public CompletionStage<Done> addPost(String blogPostId, String title, String body) {
      EntityRef<BlogCommand> entityRef = registry.entityRefFor(blogTypeKey, "blogPostId-123");
      CompletionStage<AddPostDone> result =
          entityRef.ask(replyTo -> new AddPost(new PostContent(blogPostId, title, body), replyTo), askTimeout);
      return result.thenApply(ok -> Done.getInstance());
    }
  }

  public static EntityTypeKey<BlogCommand> blogTypeKey = EntityTypeKey.create(BlogCommand.class, "BlogPost");


  interface BlogEvent {
  }
  public static class PostAdded implements BlogEvent {
    private final String postId;
    private final PostContent content;

    public PostAdded(String postId, PostContent content) {
      this.postId = postId;
      this.content = content;
    }
  }

  public static class BodyChanged implements BlogEvent {
    private final String postId;
    private final String newBody;

    public BodyChanged(String postId, String newBody) {
      this.postId = postId;
      this.newBody = newBody;
    }
  }

  public static class Published implements BlogEvent {
    private final String postId;

    public Published(String postId) {
      this.postId = postId;
    }
  }

  interface BlogState {}

  public static class BlankState implements BlogState {}

  public static class DraftState implements BlogState {
    final PostContent postContent;
    final boolean published;

    DraftState(PostContent postContent, boolean published) {
      this.postContent = postContent;
      this.published = published;
    }

    public DraftState withContent(PostContent newContent) {
      return new DraftState(newContent, this.published);
    }

    public String postId() {
      return postContent.postId;
    }
  }

  public static class PublishedState implements BlogState {
    final PostContent postContent;

    PublishedState(PostContent postContent) {
      this.postContent = postContent;
    }

    public PublishedState withContent(PostContent newContent) {
      return new PublishedState(newContent);
    }

    public String postId() {
      return postContent.postId;
    }
  }

  public interface BlogCommand {
  }
  public static class AddPost implements BlogCommand {
    final PostContent content;
    final ActorRef<AddPostDone> replyTo;

    public AddPost(PostContent content, ActorRef<AddPostDone> replyTo) {
      this.content = content;
      this.replyTo = replyTo;
    }
  }
  public static class AddPostDone implements BlogCommand {
    final String postId;

    public AddPostDone(String postId) {
      this.postId = postId;
    }
  }
  public static class GetPost implements BlogCommand {
    final ActorRef<PostContent> replyTo;

    public GetPost(ActorRef<PostContent> replyTo) {
      this.replyTo = replyTo;
    }
  }
  public static class ChangeBody implements BlogCommand {
    final String newBody;
    final ActorRef<Done> replyTo;

    public ChangeBody(String newBody, ActorRef<Done> replyTo) {
      this.newBody = newBody;
      this.replyTo = replyTo;
    }
  }
  public static class Publish implements BlogCommand {
    final ActorRef<Done> replyTo;

    public Publish(ActorRef<Done> replyTo) {
      this.replyTo = replyTo;
    }
  }
  public static class PassivatePost implements BlogCommand {
  }
  public static class PostContent implements BlogCommand {
    final String postId;
    final String title;
    final String body;

    public PostContent(String postId, String title, String body) {
      this.postId = postId;
      this.title = title;
      this.body = body;
    }
  }

  public static class BlogEntity extends PersistentEntity<BlogCommand, BlogEvent, BlogState> {

    public BlogEntity(String entityId) {
      super(blogTypeKey, entityId);
    }

    private CommandHandlerBuilder<BlogCommand, BlogEvent, BlankState, BlogState> initialCommandHandler() {
      return commandHandlerBuilder(BlankState.class)
          .matchCommand(AddPost.class, (state, cmd) -> {
            PostAdded event = new PostAdded(cmd.content.postId, cmd.content);
            return Effect().persist(event)
                .andThen(() -> cmd.replyTo.tell(new AddPostDone(cmd.content.postId)));
          });
    }

    private CommandHandlerBuilder<BlogCommand, BlogEvent, DraftState, BlogState> draftCommandHandler() {
      return commandHandlerBuilder(DraftState.class)
          .matchCommand(ChangeBody.class, (state, cmd) -> {
            BodyChanged event = new BodyChanged(state.postId(), cmd.newBody);
            return Effect().persist(event).andThen(() -> cmd.replyTo.tell(Done.getInstance()));
          })
          .matchCommand(Publish.class, (state, cmd) -> Effect()
              .persist(new Published(state.postId())).andThen(() -> {
                System.out.println("Blog post published: " + state.postId());
                cmd.replyTo.tell(Done.getInstance());
              }))
          .matchCommand(GetPost.class, (state, cmd) -> {
            cmd.replyTo.tell(state.postContent);
            return Effect().none();
          });
    }

    private CommandHandlerBuilder<BlogCommand, BlogEvent, PublishedState, BlogState> publishedCommandHandler() {
      return commandHandlerBuilder(PublishedState.class)
          .matchCommand(ChangeBody.class, (state, cmd) -> {
            BodyChanged event = new BodyChanged(state.postId(), cmd.newBody);
            return Effect().persist(event).andThen(() -> cmd.replyTo.tell(Done.getInstance()));
          })
          .matchCommand(GetPost.class, (state, cmd) -> {
            cmd.replyTo.tell(state.postContent);
            return Effect().none();
          });
    }

    private CommandHandlerBuilder<BlogCommand, BlogEvent, BlogState, BlogState> commonCommandHandler() {
      return commandHandlerBuilder(BlogState.class)
          .matchCommand(AddPost.class, (state, cmd) -> Effect().unhandled())
          .matchCommand(PassivatePost.class, (state, cmd) -> Effect().stop());
    }


    @Override
    public CommandHandler<BlogCommand, BlogEvent, BlogState> commandHandler() {
      return
          initialCommandHandler()
              .orElse(draftCommandHandler())
              .orElse(publishedCommandHandler())
              .orElse(commonCommandHandler())
              .build();
    }

    @Override
    public EventHandler<BlogState, BlogEvent> eventHandler() {
      return eventHandlerBuilder()
          .matchEvent(PostAdded.class, (state, event) ->
              new DraftState(event.content, false))
          .matchEvent(BodyChanged.class, DraftState.class, (state, chg) ->
              state.withContent(new PostContent(state.postId(), state.postContent.title, chg.newBody)))
          .matchEvent(BodyChanged.class, PublishedState.class, (state, chg) ->
              state.withContent(new PostContent(state.postId(), state.postContent.title, chg.newBody)))
          .matchEvent(Published.class, DraftState.class, (state, event) ->
              new PublishedState(state.postContent))
          .build();
    }

    @Override
    public BlogState emptyState() {
      return new BlankState();
    }

  }
}
