import scala.io.Source

object Merge {

  def main(args: Array[String]): Unit = {
    if(args.length != 5) {
      System.err.println("Usage: Merge <cfgHeader> <printHeader> <cfgParams> <file1> <file2>")
      System.exit(1)
    }
    val (cfgHeader, printHeader, cfgParams, fname1, fname2) = (args(0), args(1).toBoolean, args(2), args(3), args(4))

    val file1 = Source.fromFile(fname1).getLines().toArray
    val file2 = Source.fromFile(fname2).getLines().toArray

    val head1 = file1.head
    val head2 = file2.head

    val cols1 = head1.count(_ == ',')
    val cols2 = head2.count(_ == ',')

    if (printHeader) {
      println(s"$cfgHeader,${head1},${head2}")
    }

    1.to(math.max(file1.size -1, file2.size - 1)).foreach(i => {

      val part1 = if(i <= file1.size) file1(i) else ',' * cols1
      val part2 = if(i <= file1.size) file2(i) else ',' * cols2

      println(s"$cfgParams,$part1,$part2")
    })
  }
}