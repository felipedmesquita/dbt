require "digest"
module Dbt
  class Fingerprint
    class << self
      def generate(code)
        parsed_query = PgQuery.parse(code)

        columns = parsed_query.tree['SELECT']['target_list'].map do |column|
          column['ResTarget']['name']
        end

        columns.sort!
        columns_digest = Digest::SHA256.hexdigest(columns.join)
        query_ingerprint = parsed_query.fingerprint
        "#{query_ingerprint}_#{columns_digest}}"
      end

    end
  end
end
