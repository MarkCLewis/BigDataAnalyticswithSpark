package standardscala

object ParallelCollections extends App {
  val a = Array(4, 2, 7, 3, 9, 1).par
  println(a.aggregate(0)(_ + _, _ + _))
  // (((((0-4)-2)-7)-3)-9)-1
  // 4+(2+(7+(3+(9+(1+0)))))
  // (4+2)+(7+3)(9+1)
}