module Dbt
  module SqlTemplateHelpers

    def star relation, *exclued_columns
      columns = ActiveRecord::Base.connection.execute("select * from #{relation} limit 0").fields - exclued_columns.map(&:to_s)
      columns.map { |column| "#{relation}.#{column}" }.join(', ')
    end

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
      # x_numeric was replacing '.' and ',' to '' and '.' to convert to numeric
      # which should be done by x_numeric_comma.
      puts "WARNING: x_numeric will not change , to . in a future version. Use x_numeric_comma instead."
      #"(xpath('//cmd[@t=''#{key}'']/text()', body))[1]::numeric #{key.underscore}"
      x_numeric_comma(key)
    end

    def x_numeric_comma(key)
      "REPLACE(REPLACE((xpath('/xjx/cmd[@t=''#{key}'']/text()', body))[1]::text, '.', ''), ',', '.')::numeric #{key.underscore}"
    end

    def x_date(key)
      "TO_DATE((xpath('/xjx/cmd[@t=''#{key}'']/text()', body))[1]::text, 'DD/MM/YYYY') #{key.underscore}"
    end

    def x_except *keys
      not_in_clause = keys.map { |key| "''#{key}''" }
      .join('or @t = ')
      "xpath('//cmd[not(@t = #{not_in_clause})]', body)"
    end

  end
end
