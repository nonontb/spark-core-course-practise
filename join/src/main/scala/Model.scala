case class Movies(
                   id:Long,
                   title:String,
                   director:String)

case class Category(
                     id:Long,
                     category:String)

case class CompleteMovies(
                           id: Long,
                           title: String,
                           director: String,
                           category:String)