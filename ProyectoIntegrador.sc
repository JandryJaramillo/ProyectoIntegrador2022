import kantan.csv.ops.toCsvInputOps
import kantan.csv.rfc
import kantan.csv.generic._
import java.io.File
import scala.collection.immutable.ListMap
import play.api.libs.json.Json

case class Movies (
                    index : Int,
                    budget : Long,
                    genres : String,
                    homepage : String,
                    id : Int,
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

val path2DataFile = "C:/movie_dataset.csv"
val dataSource = new File(path2DataFile).readCsv[List, Movies](rfc.withHeader)
