import slick.jdbc.MySQLProfile.api._
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.language.postfixOps
import com.typesafe.config.ConfigFactory
import slick.collection.heterogeneous.{HCons, HList, HNil}
import slick.collection.heterogeneous.syntax._
import scala.language.postfixOps
import com.typesafe.config.ConfigFactory
import kantan.csv.ops.toCsvInputOps
import kantan.csv.rfc
import kantan.csv.generic._
import play.api.libs.json.{JsObject, Json}
import java.io.File
import scala.collection.convert.ImplicitConversions.`seq AsJavaList`
import scala.util.{Failure, Success, Try}
import play.api.libs.json._
import play.api.libs.functional.syntax._
import scala.collection.immutable.ListMap

object main extends App {

  //La ruta de la base de datos
  val db = Database.forConfig("mysql")

  // Helper method for running a query in this example file:
  def exec[T](program: DBIO[T]): T = Await.result(db.run(program), 6000 seconds)

  /*
  ======================================================================
  Creacion Tabla Movie
  ======================================================================
  */
  final case class Movie(
                          index: Int,
                          budget: Double,
                          gendre: String,
                          homepage: String,
                          id: Int,
                          keywords: String,
                          original_language: String,
                          original_title: String,
                          overview: String,
                          popularity: Double,
                          release_date: String,
                          revenue: Double,
                          runtime: Double,
                          status: String,
                          tagline: String,
                          title: String,
                          vote_average: Double,
                          vote_count: Int,
                          cast: String,
                          director: String
                        )

  final class MovieTable(tag: Tag) extends Table[Movie](tag, "movie") {

    def index = column[Int]("index")
    def budget = column[Double]("budget")
    def gendre = column[String]("gendre")
    def homepage = column[String]("homepage")
    def id = column[Int]("id", O.PrimaryKey, O.AutoInc)
    def keywords = column[String]("keywords")
    def original_language = column[String]("original_language")
    def original_title = column[String]("original_title")
    def overview = column[String]("overview")
    def popularity = column[Double]("popularity")
    def release_date = column[String]("release_date")
    def revenue = column[Double]("revenue")
    def runtime = column[Double]("runtime")
    def status = column[String]("status")
    def tagline = column[String]("tagline")
    def title = column[String]("title")
    def vote_average = column[Double]("vote_average")
    def vote_count = column[Int]("vote_count")
    def cast = column[String]("cast")
    def director = column[String]("director")

    def * = (index, budget, gendre, homepage, id, keywords, original_language, original_title, overview, popularity, release_date, revenue, runtime, status, tagline, title, vote_average, vote_count, cast, director).mapTo[Movie]
  }


  // Base query for querying the messages table:
  lazy val movies = TableQuery[MovieTable]

  println("Creating Movie table")
  //exec(movies.schema.create)

  /*
  ======================================================================
  Creacion Tabla Gender
  ======================================================================
  */
  final case class Gender(
                           id_gender: Long = 0L,
                           genres: String,
                         )

  final class GenderTable(tag: Tag) extends Table[Gender](tag, "gender") {

    def id_gender = column[Long]("id_gender", O.PrimaryKey, O.AutoInc)
    def genres = column[String]("genres")

    def * = (id_gender, genres).mapTo[Gender]
  }

  // Base query for querying the messages table:
  lazy val genress = TableQuery[GenderTable]

  println("Creating genre table")
  //exec(genress.schema.create)

  /*
  ======================================================================
  Creacion Tabla OriginalLanguage
  ======================================================================
  */
  final case class OriginalLanguage(
                                     id_originallanguage: Long = 0L,
                                     originallanguage: String,
                                   )

  final class OriginallanguageTable(tag: Tag) extends Table[OriginalLanguage](tag, "OriginalLanguage") {

    def id_originallanguage = column[Long]("id_originallanguage", O.PrimaryKey, O.AutoInc)
    def originallanguage = column[String]("originallanguage")

    def * = (id_originallanguage, originallanguage).mapTo[OriginalLanguage]
  }

  // Base query for querying the messages table:
  lazy val OriginalLanguages = TableQuery[OriginallanguageTable]

  println("Creating OriginalLanguage table")
  //exec(OriginalLanguages.schema.create)

  /*
  ======================================================================
  Creacion Tabla ProductionCompany
  ======================================================================
  */
  final case class ProductionCompany(
                                      id_productioncompany: Long = 0L,
                                      productioncompany: String,
                                    )

  final class ProductionCompanyTable(tag: Tag) extends Table[ProductionCompany](tag, "ProductionCompany") {

    def id_productioncompany = column[Long]("id_productioncompany", O.PrimaryKey)
    def productioncompany = column[String]("productioncompany")

    def * = (id_productioncompany, productioncompany).mapTo[ProductionCompany]
  }

  // Base query for querying the messages table:
  lazy val ProductionCompanies = TableQuery[ProductionCompanyTable]

  println("Creating ProductionCompanie table")
  //exec(ProductionCompanies.schema.create)

  /*
  ======================================================================
  Creacion Tabla ProductionCountries
  ======================================================================
  */
  final case class ProductionCountries(
                                        id_productioncountry: Long = 0L,
                                        iso_3166_1: String,
                                        name: String)

  final class ProductionCountriesTable(tag: Tag) extends Table[ProductionCountries](tag, "production_countries") {

    def id_productioncountry = column[Long]("id_productioncountry", O.PrimaryKey)
    def iso_3166_1 = column[String]("iso_3166_1")
    def name = column[String]("name")

    def * = (id_productioncountry ,iso_3166_1, name).mapTo[ProductionCountries]
  }

  // Base query for querying the messages table:
  lazy val productionCountriess = TableQuery[ProductionCountriesTable]

  println("Creating productionCountries table")
  //exec(productionCountriess.schema.create)

  /*
  ======================================================================
  Creacion Tabla Cast
  ======================================================================
  */
  final case class Cast(
                         id_cast: Long = 0L,
                         authorName: String,
                       )

  final class CastTable(tag: Tag) extends Table[Cast](tag, "cast") {

    def id_cast = column[Long]("id_cast", O.PrimaryKey, O.AutoInc)
    def authorName = column[String]("authorName")

    def * = (id_cast, authorName).mapTo[Cast]
  }

  // Base query for querying the messages table:
  lazy val cast = TableQuery[CastTable]

  println("Creating director table")
  //exec(cast.schema.create)

  /*
  ======================================================================
  Creacion Tabla SpokenLanguages
  ======================================================================
  */
  final case class SpokenLanguages(
                                    id_spokenlanguages: Long = 0L,
                                    iso: String, name: String)

  final class SpokenLanguagesTable(tag: Tag) extends Table[SpokenLanguages](tag, "spoken_languages") {

    def id_spokenlanguages = column[Long]("id_spokenlanguages", O.PrimaryKey)
    def iso = column[String]("iso")
    def name = column[String]("name")

    def * = (id_spokenlanguages,iso, name).mapTo[SpokenLanguages]
  }

  // Base query for querying the messages table:
  lazy val SpokenLanguagess = TableQuery[SpokenLanguagesTable]

  println("Creating SpokenLanguages table")
  //exec(SpokenLanguagess.schema.create)

  /*
  ======================================================================
  Creacion Tabla Crew
  ======================================================================
  */
  case class Crew (
                    crewName : String,
                    genderCrew : Int,
                    credit_idCrew : String,
                    idCrew : Long = 0l
                  )

  final class CrewTable(tag: Tag) extends Table[Crew](tag, "crew") {

    def crewName = column[String]("crewName")
    def genderCrew = column[Int]("genderCrew")
    def credit_idCrew = column[String]("credit_idCrew")
    def idCrew = column[Long]("idCrew", O.PrimaryKey, O.AutoInc)

    def * = ( crewName, genderCrew, credit_idCrew, idCrew).mapTo[Crew]
  }

  lazy val crew = TableQuery[CrewTable]

  println("Creating crew table")
  //exec(crew.schema.create)

  /*
  ======================================================================
  Creacion Tabla DepartmentCrew
  ======================================================================
  */
  case class DepartmentCrew (
                              idDepartment : Long = 0l,
                              departCrewName : String,
                            )

  final class DepartmentTable(tag: Tag) extends Table[DepartmentCrew](tag, "DepartmentCrew") {

    def idDepartment = column[Long]("idDepartment", O.PrimaryKey, O.AutoInc)
    def departCrewName = column[String]("departCrewName")

    def * = (idDepartment, departCrewName).mapTo[DepartmentCrew]
  }

  lazy val departmentCrew = TableQuery[DepartmentTable]

  println("Creating departmentCrew table")
  //exec(departmentCrew.schema.create)

  /*
  ======================================================================
  Creacion Tabla JobCrew
  ======================================================================
  */
  case class JobCrew (
                       idJob : Long = 0l,
                       jobName : String,
                     )

  final class JobTable(tag: Tag) extends Table[JobCrew](tag, "JobCrew") {

    def idJob = column[Long]("idJob", O.PrimaryKey, O.AutoInc)
    def jobName = column[String]("jobName")

    def * = (idJob, jobName).mapTo[JobCrew]
  }

  lazy val jobcrew = TableQuery[JobTable]


  println("Creating jobcrew table")
  //exec(jobcrew.schema.create)

  /*
  ======================================================================
  Creacion Tabla Director
  ======================================================================
  */
  final case class Director(
                             id_director: Long = 0L,
                             director: String,
                           )

  final class DirectorTable(tag: Tag) extends Table[Director](tag, "director") {

    def id_director = column[Long]("id_director", O.PrimaryKey, O.AutoInc)
    def director = column[String]("director")

    def * = (id_director, director).mapTo[Director]
  }

  // Base query for querying the messages table:
  lazy val directors = TableQuery[DirectorTable]

  println("Creating director table")
  //exec(directors.schema.create)

  /*
  ====================================================================================================
  ====================================================================================================
                                             Insert Data
  ===================================================================================================
  */
  case class Movies (
                      index : Int,
                      budget : Int,
                      genres : String,
                      homepage : String,
                      id : String,
                      keywords : String,
                      original_language : String,
                      original_title : String,
                      overview : String,
                      popularity : Double,
                      production_companies : String,
                      production_countries : String,
                      release_date : String,
                      revenue : Long,
                      runtime : Option[Double],
                      spoken_language : String,
                      status : String,
                      tagline : String,
                      title : String,
                      vote_average : Double,
                      vote_count : Int,
                      cast : String,
                      crew : String,
                      director : String
                    )

  val path2DataFile4 = "C:\\movie_dataset.csv"
  val dataSource = new File(path2DataFile4).readCsv[List, Movies ](rfc.withHeader)
  val valuesRight = dataSource.collect({ case Right(movies) => movies})

  /*
    ==============
      Insert Crew
    ==============
   */
/*
  def replacement(crew : String) : String = {
    Seq(
      "\\{'" -> "{\"",
      "': '" -> "\": \"",
      "', '" -> "\", \"",
      ", '" -> ", \"",
      "': " -> "\": "
    ).foldLeft(crew){case (z, (s, r)) => z.replaceAll(s, r)}
  }

  val crewTryValues = valuesRight
    .map(r => Try(Json.parse(replacement(r.crew))))

  val crewValidV2 = crewTryValues.collect({case Success(su) => su})

  val crewList = crewValidV2.flatten(crewMovie => crewMovie.as[JsArray].value)
    .map(crewJS => Crew(
      (crewJS \ "crewName").as[String],
      (crewJS \ "genderCrew").as[Int],
      (crewJS \ "credit_idCrew").as[String],
      (crewJS \ "idCrew").as[Long],
    ))

  implicit val crewReads : Reads[Crew] = (
    (JsPath \ "crewName").read[String] and
      (JsPath \ "genderCrew").read[Int] and
      (JsPath \ "credit_idCrew").read[String] and
      (JsPath \ "idCrew").read[Long]
    ) (Crew.apply _)

  val crewList2 = crewValidV2.flatten(crewMovie => crewMovie.as[List[Crew]]).distinct
*/

  val path = "./src/main/resources/crew.csv"
  val dataSource2 = new File(path).readCsv[List, Crew ](rfc.withHeader)
  val valuesRight2 = dataSource2.collect({ case Right(crew) => crew})
  //Create and insert the crew data:
  println("\nInserting crew data")
  exec(crew ++= valuesRight2)

  /*
  ==============
    Insert listProdCompanies
  ==============
 */
  val listProdCompanies  = valuesRight
    .map(pcomp => Json.parse(pcomp.production_companies))
    .flatten(arr => arr.as[JsArray].value)
    .map(obj => ((obj \ "name").as[String], (obj \ "id").as[Int]))
    .distinct
/*
  val out = java.io.File.createTempFile("production_companies.csv","csv")
  out.writeCsv(listProdComp, rfc.withHeader("name", "id"))

  //grabar info
  val out = new File("C:\\Users\\USUARIO\\Desktop\\Proyecto Integrador\\crew.csv")
  out.writeCsv(crewList2, rfc.withHeader("name", "gender", "department", "job", "credit_id", "id"))
*/
}
