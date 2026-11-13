package project

object Skeleton {
  def main(args: Array[String]): Unit = {
    require(
      args.length == 1,
      "Usage: Skeleton <input-jsonl-path>"
    )
    
    println("This is an empty project.")
  }
}
