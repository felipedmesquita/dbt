module Dbt
  module SqlTemplateHelpers

    ### JSON SQL helpers
    def j(key)
      "body ->> '#{key}' #{key.underscore}"
    end

    def j_numeric(key)
      "(body ->> '#{key}')::numeric #{key.underscore}"
    end

    def j_numeric_comma(key)
      "REPLACE(REPLACE((body ->> '#{key}'),'.',''),',','.')::numeric #{key.underscore}"
    end

    def j_except *keys
      "body - '#{keys.join("','")}'"
    end

    ### XML SQL helpers
    def x(key)
      "(xpath('//cmd[@t=''#{key}'']/text()', body))[1]::text AS #{key.underscore}"
    end

    def x_numeric(key)
      "REPLACE(REPLACE((xpath('/xjx/cmd[@t=''#{key}'']/text()', body))[1]::text, '.', ''), ',', '.')::numeric #{key.underscore}"
    end

    def x_date(key)
      "TO_DATE((xpath('/xjx/cmd[@t=''#{key}'']/text()', body))[1]::text, 'DD/MM/YYYY') #{key.underscore}"
    end

    def x_except *keys
      not_in_clause = keys.map { |key| "''#{key}''" }
      .join('or @t = ')
      "xpath('//cmd[not(@t = #{not_in_clause})]', xml_completo)"
    end

  end
end
