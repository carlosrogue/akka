/**
 * Copyright (C) 2018 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.typed.javadsl

import java.util.Optional
import java.util.function.BiFunction
import java.util.function.{ Function ⇒ JFunction }

import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.BackoffSupervisorStrategy
import akka.actor.typed.Behavior
import akka.actor.typed.Props
import akka.annotation.InternalApi
import akka.cluster.sharding.ShardCoordinator.ShardAllocationStrategy
import akka.cluster.sharding.typed.ClusterShardingSettings
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.cluster.sharding.typed.ShardingMessageExtractor
import akka.persistence.typed.javadsl.PersistentBehavior

// FIXME docs
abstract class PersistentEntity[Command, Event, State >: Null] private (
  val typeKey:   EntityTypeKey[Command],
  persistenceId: String, supervisorStrategy: Option[BackoffSupervisorStrategy])
  extends PersistentBehavior[Command, Event, State](persistenceId: String, supervisorStrategy: Option[BackoffSupervisorStrategy]) {

  def this(typeKey: EntityTypeKey[Command], entityId: String) = {
    this(typeKey, persistenceId = typeKey.persistenceIdFrom(entityId), None)
  }

  def this(typeKey: EntityTypeKey[Command], entityId: String, backoffSupervisorStrategy: BackoffSupervisorStrategy) = {
    this(typeKey, persistenceId = typeKey.persistenceIdFrom(entityId), Some(backoffSupervisorStrategy))
  }

}

object PersistentEntityRegistry {
  def get(system: ActorSystem[_]): PersistentEntityRegistry =
    new PersistentEntityRegistry(ClusterSharding.get(system))

}

class PersistentEntityRegistry(sharding: ClusterSharding) {
  /**
   * Initialize sharding for the given `shardedEntity` factory settings.
   *
   * It will start a shard region or a proxy depending on if the settings require role and if this node has
   * such a role.
   *
   * @tparam M The type of message the entity accepts
   * @tparam E A possible envelope around the message the entity accepts
   */
  def start[M, E](shardedPersistentEntity: ShardedPersistentEntity[M, E]): ActorRef[E] =
    sharding.start(shardedPersistentEntity.toShardedEntity)

  /**
   * Create an `ActorRef`-like reference to a specific sharded entity.
   * Currently you have to correctly specify the type of messages the target can handle.
   *
   * Messages sent through this [[EntityRef]] will be wrapped in a [[ShardingEnvelope]] including the
   * here provided `entityId`.
   *
   * For in-depth documentation of its semantics, see [[EntityRef]].
   */
  def entityRefFor[M](typeKey: EntityTypeKey[M], entityId: String): EntityRef[M] =
    sharding.entityRefFor(typeKey, entityId)
}

object ShardedPersistentEntity {

  /**
   * Defines how the entity should be created. Used in [[PersistentEntityRegistry#start]]. More optional
   * settings can be defined using the `with` methods of the returned [[ShardedPersistentEntity]].
   *
   * @param createEntity Create the [[PersistentEntity]] for an entity given an entityId
   * @param typeKey A key that uniquely identifies the type of entity in this cluster
   * @param stopMessage Message sent to an entity to tell it to stop, e.g. when rebalanced or passivated.
   *
   * @tparam Command The type of message the entity accepts
   */
  def create[Command, Event, State >: Null](
    createEntity: JFunction[String, PersistentEntity[Command, Event, State]],
    typeKey:      EntityTypeKey[Command],
    stopMessage:  Command): ShardedPersistentEntity[Command, ShardingEnvelope[Command]] = {
    create(new BiFunction[ActorRef[ClusterSharding.ShardCommand], String, PersistentEntity[Command, Event, State]] {
      override def apply(shard: ActorRef[ClusterSharding.ShardCommand], entityId: String): PersistentEntity[Command, Event, State] =
        createEntity.apply(entityId)
    }, typeKey, stopMessage)
  }

  /**
   * Defines how the entity should be created. Used in [[PersistentEntityRegistry#start]]. More optional
   * settings can be defined using the `with` methods of the returned [[ShardedPersistentEntity]].
   *
   * @param createEntity Create the behavior for an entity given `ShardCommand` ref and an entityId
   * @param typeKey A key that uniquely identifies the type of entity in this cluster
   * @param stopMessage Message sent to an entity to tell it to stop, e.g. when rebalanced or passivated.
   *
   * @tparam Command The type of message the entity accepts
   */
  def create[Command, Event, State >: Null](
    createEntity: BiFunction[ActorRef[ClusterSharding.ShardCommand], String, PersistentEntity[Command, Event, State]],
    typeKey:      EntityTypeKey[Command],
    stopMessage:  Command): ShardedPersistentEntity[Command, ShardingEnvelope[Command]] = {

    new ShardedPersistentEntity(new BiFunction[ActorRef[ClusterSharding.ShardCommand], String, Behavior[Command]] {
      override def apply(shard: ActorRef[ClusterSharding.ShardCommand], entityId: String): Behavior[Command] = {
        val persistentEntity = createEntity.apply(shard, entityId)
        if (persistentEntity.typeKey != typeKey)
          throw new IllegalArgumentException(s"The [${persistentEntity.typeKey}] of the PersistentEntity " +
            s" [${persistentEntity.getClass.getName}] doesn't match expected $typeKey.")
        persistentEntity
      }
    }, typeKey, stopMessage, Props.empty, Optional.empty(), Optional.empty(), Optional.empty())
  }

  // FIXME those createEntity factory functions would also need ActorContext parameter, probably better to
  // have only one with a PersistentEntityContext parameter, that holds ActorContext, entityId, ActorRef[ClusterSharding.ShardCommand]
}

// FIXME DRY ShardedPersistentEntity with ShardedEntity

/**
 * Defines how the entity should be created. Used in [[PersistentEntityRegistry#start]].
 */
final class ShardedPersistentEntity[M, E] private[akka] (
  val createBehavior:     BiFunction[ActorRef[ClusterSharding.ShardCommand], String, Behavior[M]],
  val typeKey:            EntityTypeKey[M],
  val stopMessage:        M,
  val entityProps:        Props,
  val settings:           Optional[ClusterShardingSettings],
  val messageExtractor:   Optional[ShardingMessageExtractor[E, M]],
  val allocationStrategy: Optional[ShardAllocationStrategy]) {

  /**
   * [[akka.actor.typed.Props]] of the entity actors, such as dispatcher settings.
   */
  def withEntityProps(newEntityProps: Props): ShardedPersistentEntity[M, E] =
    copy(entityProps = newEntityProps)

  /**
   * Additional settings, typically loaded from configuration.
   */
  def withSettings(newSettings: ClusterShardingSettings): ShardedPersistentEntity[M, E] =
    copy(settings = Optional.ofNullable(newSettings))

  /**
   *
   * If a `messageExtractor` is not specified the messages are sent to the entities by wrapping
   * them in [[ShardingEnvelope]] with the entityId of the recipient actor. That envelope
   * is used by the [[HashCodeMessageExtractor]] for extracting entityId and shardId. The number of
   * shards is then defined by `numberOfShards` in `ClusterShardingSettings`, which by default
   * is configured with `akka.cluster.sharding.number-of-shards`.
   */
  def withMessageExtractor[Envelope](newExtractor: ShardingMessageExtractor[Envelope, M]): ShardedPersistentEntity[M, Envelope] =
    new ShardedPersistentEntity(createBehavior, typeKey, stopMessage, entityProps, settings, Optional.ofNullable(newExtractor), allocationStrategy)

  /**
   * Allocation strategy which decides on which nodes to allocate new shards,
   * [[ClusterSharding#defaultShardAllocationStrategy]] is used if this is not specified.
   */
  def withAllocationStrategy(newAllocationStrategy: ShardAllocationStrategy): ShardedPersistentEntity[M, E] =
    copy(allocationStrategy = Optional.ofNullable(newAllocationStrategy))

  private def copy(
    create:             BiFunction[ActorRef[ClusterSharding.ShardCommand], String, Behavior[M]] = createBehavior,
    typeKey:            EntityTypeKey[M]                                                        = typeKey,
    stopMessage:        M                                                                       = stopMessage,
    entityProps:        Props                                                                   = entityProps,
    settings:           Optional[ClusterShardingSettings]                                       = settings,
    allocationStrategy: Optional[ShardAllocationStrategy]                                       = allocationStrategy
  ): ShardedPersistentEntity[M, E] = {
    new ShardedPersistentEntity(create, typeKey, stopMessage, entityProps, settings, messageExtractor, allocationStrategy)
  }

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def toShardedEntity: ShardedEntity[M, E] =
    new ShardedEntity(createBehavior, typeKey, stopMessage, entityProps, settings, messageExtractor, allocationStrategy)

}
