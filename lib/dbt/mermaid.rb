require 'base64'
module Dbt
  class Mermaid
    class << self

      def markdown_for dag
        mermaid = "graph TD\n"
        dag.each do |model, dependencies|
          mermaid += "#{model}\n"
          dependencies.each do |dependency|
            mermaid += "#{dependency} --> #{model}\n"
          end
        end
        mermaid
      end

      def encode_to_editor_url md
        json = {code: md, :mermaid=>{theme: "default"}, :updateEditor=>false, :autoSync=>true, :updateDiagram=>false}
        encoded = Base64.urlsafe_encode64(json.to_json.force_encoding('ASCII'))
        "https://mermaid.ink/img/#{encoded}"
        #url = encode_mermaid(diagram)
        encoded
      end

      def generate_file chart
        html = <<~HTML
          <!DOCTYPE html>
            <html lang="en">
              <body>
                <pre class="mermaid">
                #{chart}
                </pre>
                <script type="module">
                  import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs';
                </script>
              </body>
            </html>
            HTML

        File.write('dependencies.html', html)
      end

    end

  end
end
