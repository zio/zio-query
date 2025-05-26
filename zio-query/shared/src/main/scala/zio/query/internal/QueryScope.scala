package zio.query.internal

import zio.*
import zio.stacktracer.TracingImplicits.disableAutoTrace

import java.util.concurrent.atomic.AtomicReference

/**
 * Lightweight variant of [[zio.Scope]], optimized for usage with ZQuery
 */
sealed trait QueryScope {
  def addFinalizerExit(f: Exit[Any, Any] => UIO[Any])(implicit trace: Trace): UIO[Unit]
  def closeAndExitWith[E, A](exit: Exit[E, A])(implicit trace: Trace): IO[E, A]
}

private[query] object QueryScope {
  def make(): QueryScope = new Default

  case object NoOp extends QueryScope {
    def addFinalizerExit(f: Exit[Any, Any] => UIO[Any])(implicit trace: Trace): UIO[Unit] = ZIO.unit
    def closeAndExitWith[E, A](exit: Exit[E, A])(implicit trace: Trace): IO[E, A]         = exit
  }

  final private class Default extends QueryScope {
    private val ref = new AtomicReference(List.empty[Exit[Any, Any] => UIO[Any]])

    def addFinalizerExit(f: Exit[Any, Any] => UIO[Any])(implicit trace: Trace): UIO[Unit] =
      ZIO.succeed {
        ref.updateAndGet(f :: _)
        ()
      }

    def closeAndExitWith[E, A](exit: Exit[E, A])(implicit trace: Trace): IO[E, A] = {
      val finalizers = ref.get
      if (finalizers.isEmpty) exit
      else {
        ref.set(Nil)
        ZIO.foreachDiscard(finalizers)(_(exit)) *> exit
      }
    }
  }
}
