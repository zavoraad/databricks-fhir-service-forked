/*
 1. optimized for multiple connections
    -JDBC Pool (monolithic tenancy)
 2. Specific input/output for queries


 6. On Behlf Of... Multi tenancy... 



case class QueryInput(query: String, oboUser: String)

case class QueryOutput(queryResults: String, queryRuntime: long, queryStartTime: DateTime)


class QueryRunner(auth: Authorizaiton){

      def runQuery(queryInput: QueryInput): QueryOutput = {
      	  ??? 
      } 
}
 */
