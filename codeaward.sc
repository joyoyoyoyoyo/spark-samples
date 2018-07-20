case class Demographic(id: Int,
                       age: Int,
                       codingBootcamp: Boolean,
                       country: String,
                       gender: String,
                       isEthnicMinority: Boolean,
                       servedInMilitary: Boolean)
val demographics = sc.textFile("demographics.txt").map(line => {
                       val arr = line.split(",")
                       val demographic =
                        Demographic(id = arr(0).toInt,
                                   age = arr(1).toInt,
                                   codingBootcamp = arr(2).toBoolean,
                                   country = arr(3),
                                   gender = arr(4),
                                   isEthnicMinority = arr(5).toBoolean,
                                   servedInMilitary = arr(6).toBoolean)
                       (demographic.id, demographic)
                     })

case class Finances(id: Int,
                    hasDebt: Boolean,
                    hasFinancialDependents: Boolean,
                    hasStudentLoans: Boolean,
                    income: Int)
val finances = sc.textFile("finances.txt").map(line => {
                   val arr = line.split(",")
                   val finances =
                     Finances(id = arr(0).toInt,
                              hasDebt = arr(1).toBoolean,
                              hasFinancialDependents = arr(2).toBoolean,
                              hasStudentLoans = arr(3).toBoolean,
                              income = arr(4).toInt)
                  (finances.id, finances)
                })


// Possibility 1
demographics.join(finances).filter { p =>
              p._2._1.country == "Switzerland" &&
              p._2._2.hasFinancialDependents &&
              p._2._2.hasDebt
            }.count


// Possibility 2
val filtered = finances.filter(p => p._2.hasFinancialDependents && p._2.hasDebt)

demographics.filter(p => p._2.country == "Switzerland").join(filtered).count

// Possibility 3
val cartesian = demographics.cartesian(finances)

cartesian.filter {
  case (p1, p2) => p1._1 == p2._1
}.filter {
  case (p1, p2) => (p1._2.country == "Switzerland") &&
                   (p2._2.hasFinancialDependents) &&
                   (p2._2.hasDebt)
}.count