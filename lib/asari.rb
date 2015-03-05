require "asari/version"

require "asari/collection"
require "asari/exceptions"
require "asari/geography"

require "httparty"

require "ostruct"
require "json"
require "cgi"

class Asari
  DEFAULT_SIZE = 10

  def self.mode
    @@mode
  end

  def self.mode=(mode)
    @@mode = mode
  end

  attr_writer :api_version
  attr_writer :search_domain
  attr_writer :aws_region

  def initialize(search_domain=nil, aws_region=nil, api_version=nil)
    @search_domain = search_domain
    @aws_region = aws_region
    @api_version = api_version
  end

  # Public: returns the current search_domain, or raises a
  # MissingSearchDomainException.
  #
  def search_domain
    @search_domain || raise(MissingSearchDomainException.new)
  end

  # Public: returns the current api_version, or the sensible default of
  # "2011-02-01" (at the time of writing, the current version of the
  # CloudSearch API).
  #
  def api_version
    @api_version || ENV['CLOUDSEARCH_API_VERSION'] || "2011-02-01" 
  end

  # Public: returns the current aws_region, or the sensible default of
  # "us-east-1."
  def aws_region
    @aws_region || "us-east-1"
  end

  # Public: Search for the specified term.
  #
  # Examples:
  #
  #     @asari.search("fritters") #=> ["13","28"]
  #     @asari.search(filter: { and: { type: 'donuts' }}) #=> ["13,"28","35","50"]
  #     @asari.search("fritters", filter: { and: { type: 'donuts' }}) #=> ["13"]
  #
  # Returns: An Asari::Collection containing all document IDs in the system that match the
  #   specified search term. If no results are found, an empty Asari::Collection is
  #   returned.
  #
  # Raises: SearchException if there's an issue communicating the request to
  #   the server.
  def search(*terms)
    case api_version
      when '2011-02-01'
        search_with_2011_02_01_api(*terms)
      when '2013-01-01'
        search_with_2013_01_01_api(*terms)
      else
        search_with_latest_api(*terms)
    end
  end

  def search_with_2011_02_01_api(term, options = {})
    return Asari::Collection.sandbox_fake if self.class.mode == :sandbox
    term,options = "",term if term.is_a?(Hash) and options.empty?

    bq = boolean_query(options[:filter]) if options[:filter]
    page_size = options[:page_size].nil? ? 10 : options[:page_size].to_i

    url = "http://search-#{search_domain}.#{aws_region}.cloudsearch.amazonaws.com/2011-02-01/search"

    url += "?q=#{CGI.escape(term.to_s)}"
    url += "&bq=#{CGI.escape(bq)}" if options[:filter]

    url += "&size=#{page_size}"
    url += "&return-fields=#{options[:return_fields].join ','}" if options[:return_fields]

    if options[:page]
      start = (options[:page].to_i - 1) * page_size
      url << "&start=#{start}"
    end

    if options[:rank]
      rank = normalize_rank(options[:rank])
      url << "&rank=#{CGI.escape(rank)}"
    end

    begin
      response = HTTParty.get(url)
    rescue Exception => e
      ae = Asari::SearchException.new("#{e.class}: #{e.message} (#{url})")
      ae.set_backtrace e.backtrace
      raise ae
    end

    unless response.response.code == "200"
      raise Asari::SearchException.new("#{response.response.code}: #{response.response.msg} (#{url})")
    end

    Asari::Collection.new(response, page_size)
  end

  def search_with_2013_01_01_api(*terms)
    return Asari::Collection.sandbox_fake if self.class.mode == :sandbox

    options = terms.extract_options!

    endpoint = "http://search-#{search_domain}.#{aws_region}.cloudsearch.amazonaws.com/2013-01-01/search"
    params = []

    q = structured_query(options[:query] || terms, compound: true)
    params << "q=#{CGI.escape(q)}"

    if options[:filter]
      fq = structured_query(options[:filter], compound: true)

      params << "fq=#{CGI.escape(fq)}&q.parser=structured"
    end

    returning = extract_returning(options)
    params << "return=#{returning.join(',')}" if returning.present?

    start, size = extract_pagination(options)
    params << "start=#{start.to_i}" if start.present? and not start.zero?
    params << "size=#{size.to_i}"

    sort = extract_sorting(options)
    params << "sort=#{CGI.escape(sort)}" if sort.present?

    url = [endpoint, params.join('&')].join('?')

    begin
      response = HTTParty.get(url)
    rescue Exception => e
      ae = Asari::SearchException.new("#{e.class}: #{e.message} (#{url})")
      ae.set_backtrace e.backtrace
      raise ae
    end

    unless response.response.code == "200"
      raise Asari::SearchException.new("#{response.response.code}: #{response.response.msg} (#{url})")
    end

    Asari::Collection.new(response, size)
  end
  alias_method :search_with_latest_api, :search_with_2013_01_01_api

  # Public: Add an item to the index with the given ID.
  #
  #     id - the ID to associate with this document
  #     fields - a hash of the data to associate with this document. This
  #       needs to match the search fields defined in your CloudSearch domain.
  #
  # Examples:
  #
  #     @asari.update_item("4", { :name => "Party Pooper", :email => ..., ... }) #=> nil
  #
  # Returns: nil if the request is successful.
  #
  # Raises: DocumentUpdateException if there's an issue communicating the
  #   request to the server.
  #
  def add_item(id, fields)
    return nil if self.class.mode == :sandbox
    query = create_item_query id, fields
    doc_request(query)
  end

  # Public: Update an item in the index based on its document ID.
  #   Note: As of right now, this is the same method call in CloudSearch
  #   that's utilized for adding items. This method is here to provide a
  #   consistent interface in case that changes.
  #
  # Examples:
  #
  #     @asari.update_item("4", { :name => "Party Pooper", :email => ..., ... }) #=> nil
  #
  # Returns: nil if the request is successful.
  #
  # Raises: DocumentUpdateException if there's an issue communicating the
  #   request to the server.
  #
  def update_item(id, fields)
    add_item(id, fields)
  end

  # Public: Remove an item from the index based on its document ID.
  #
  # Examples:
  #
  #     @asari.search("fritters") #=> ["13","28"]
  #     @asari.remove_item("13") #=> nil
  #     @asari.search("fritters") #=> ["28"]
  #     @asari.remove_item("13") #=> nil
  #
  # Returns: nil if the request is successful (note that asking the index to
  #   delete an item that's not present in the index is still a successful
  #   request).
  # Raises: DocumentUpdateException if there's an issue communicating the
  #   request to the server.
  def remove_item(id)
    return nil if self.class.mode == :sandbox

    query = remove_item_query id
    doc_request query
  end

  # Internal: helper method: common logic for queries against the doc endpoint.
  #
  def doc_request(query)
    request_query = query.class.name == 'Array' ? query : [query]
    endpoint = "http://doc-#{search_domain}.#{aws_region}.cloudsearch.amazonaws.com/#{api_version}/documents/batch"

    options = { :body => request_query.to_json, :headers => { "Content-Type" => "application/json"} }

    begin
      response = HTTParty.post(endpoint, options)
    rescue Exception => e
      ae = Asari::DocumentUpdateException.new("#{e.class}: #{e.message}")
      ae.set_backtrace e.backtrace
      raise ae
    end

    unless response.response.code == "200"
      raise Asari::DocumentUpdateException.new("#{response.response.code}: #{response.response.msg}")
    end

    nil
  end

  def create_item_query(id, fields)
    return nil if self.class.mode == :sandbox
    query = { "type" => "add", "id" => id.to_s, "version" => Time.now.to_i, "lang" => "en" }
    fields.each do |k,v|
      fields[k] = convert_date_or_time(fields[k])
      fields[k] = "" if v.nil?
    end

    query["fields"] = fields
    query
  end

  def remove_item_query(id)
    { "type" => "delete", "id" => id.to_s, "version" => Time.now.to_i }
  end

  protected

  def extract_pagination(options = {})
    start = options[:start]
    size  = options[:size]
    page  = options[:page]
    per   = options[:per]

    case
      when (start and size)
        [start, size]
      when (page and per)
        [(page - 1) * per, per]
      when (size or per)
        [nil, (size || per)]
      else
        [nil, DEFAULT_SIZE]
    end
  end

  def extract_sorting(options = {})
    sort = options[:sort]

    case sort
      when Hash
        "#{sort[:by] || '_score'} #{sort[:order] || 'desc'}"
      when String
        "#{sort} desc"
      when Symbol
        "#{sort} desc"
      else
        nil
    end
  end

  def extract_returning(options = {})
    returning = options[:return]

    case returning
      when Array
        returning
      when String
        [returning]
      when Symbol
        [returning.to_s]
      else
        []
    end
  end


  # Private: Builds the query from a passed hash
  #
  #     terms - a hash of the search query. %w(and or not) are reserved hash keys
  #             that build the logic of the query
  def boolean_query(terms = {}, options = {})
    reduce = lambda { |hash|
      hash.reduce("") do |memo, (key, value)|
        if %w(and or not).include?(key.to_s) && value.is_a?(Hash)
          sub_query = reduce.call(value)
          memo += "(#{key}#{sub_query})" unless sub_query.empty?
        else
          if value.is_a?(Range) || value.is_a?(Integer)
            memo += " #{key}:#{value}"
          else
            memo += " #{key}:'#{value}'" unless value.to_s.empty?
          end
        end
        memo
      end
    }
    reduce.call(terms)
  end


  # Private: Builds the query from a passed hash
  #
  #     terms - a hash of the search query. %w(and or not) are reserved hash keys
  #             that build the logic of the query
  def structured_query(expression, options = {})
    case expression
      when Hash
        expression.reduce("") do |memo, (key, value)|
          case key
            when 'and'
              memo + "(and #{structured_query(value, options.dup.merge(key: :and))})"
            when 'or'
              memo + "(or #{structured_query(value, options.dup.merge(key: :or))})"
            when 'not'
              memo + "(not #{structured_query(value, options.dup.merge(key: :not))})"
            when 'range'
              memo + structured_range(value, nil, options[:compound])
            when 'prefix'
              memo + structured_prefix(value, nil, options[:compound])
            else
              case value
                when Hash
                  if value.has_key?(:min) or value.has_key?(:max)
                    memo + structured_range(value, key, options[:compound])
                  elsif value.has_key?(:prefix)
                    memo + structured_prefix(value, key, options[:compound])
                  else
                    raise "Could not guess what to do for #{key}"
                  end
                when Array
                  case options[:default_operator]
                    when :and
                      memo + "(and #{structured_query(value, options.dup.merge(key: :and))})"
                    when :or
                      memo + "(or #{structured_query(value, options.dup.merge(key: :or))})"
                    else
                      memo + "(or #{structured_query(value, options.dup.merge(key: :or))})"
                  end
                when Range
                  memo + structured_range(value, key, options[:compound])
                when String
                  memo + "#{key}:#{convert_for_cloud_search value}"
                when Symbol
                  memo + "#{key}:#{convert_for_cloud_search value}"
                when Numeric
                  memo + "#{key}:#{convert_for_cloud_search value}"
                when Date
                  memo + "#{key}:#{convert_for_cloud_search value}"
                else
                  raise "Could not guess what to do for #{key}"
              end
          end
        end
      when Array
        key = options.delete(:key)

        if key
          expression.map do |exp|
            structured_query(exp, options)
          end.join(' ')
        else
          return structured_query(expression.first, options) if expression.one?

          case options[:default_operator]
            when :and
              "(and #{structured_query(expression, options.dup.merge(key: :and))})"
            when :or
              "(or #{structured_query(expression, options.dup.merge(key: :or))})"
            else
              "(or #{structured_query(expression, options.dup.merge(key: :or))})"
          end
        end
      when String
        if expression.last == '*'
          structured_prefix(expression, nil, options[:compound])
        else
          "'#{expression}'"
        end
      when Symbol
        "'#{expression}'"
      when Numeric
        "#{expression}"
      else
        raise "Unknown expression Type #{expression.class}"
    end
  end

  def structured_prefix(expression, field = nil, compound = true)
    case expression
      when Array
        field ||= expression.extract_options![:field]
      when Hash
        field ||= expression[:field]
      else
    end

    if compound
      " (prefix#{" field:#{field}" if field} '#{format_prefix(expression)}')"
    else
      " #{format_prefix(expression)}*"
    end
  end

  def format_prefix(expression)
    case expression
      when Hash
        convert_for_prefix expression[:prefix]
      when Array
        convert_for_prefix expression.first
      when String
        convert_for_prefix expression
      else
    end
  end

  def structured_range(expression, field = nil, compound = true)
    case expression
      when Array
        field ||= expression.extract_options![:field]
      when Hash
        field ||= expression[:field]
      else
    end

    raise 'No target field provided for range' unless field

    if compound
      " (range field:#{field} #{format_range(expression)})"
    else
      " #{field}:#{format_range(expression)}"
    end
  end

  def format_range(expression)
    case expression
      when Hash
        min = convert_for_range expression[:min]
        max = convert_for_range expression[:max]

        "#{min.present? ? "[#{min}" : '{' },#{max.present? ? "#{max}]" : '}'}"
      when Array
        min = convert_for_range expression.first
        max = convert_for_range expression.last

        "#{min.present? ? "[#{min}" : '{' },#{max.present? ? "#{max}]" : '}'}"
      when Range
        min = convert_for_range expression.first
        max = convert_for_range expression.last

        "#{min.present? ? "[#{min}" : '{' },#{max.present? ? "#{max}#{expression.exclude_end? ? '}' : ']' }" : '}'}"
      else
        raise "Unknown expression Type #{expression.class}"
    end
  end

  def convert_for_cloud_search(value)
    case value
      when DateTime
        value.utc.strftime('%Y-%m-%dT%H:%M:%SZ')
      when Date
        value.strftime('%Y-%m-%dT%H:%M:%SZ')
      when Numeric
        value.to_i
      else
        "'#{value.to_s}'"
    end
  end

  def convert_for_range(value)
    if value.class.in?([Date, DateTime, Fixnum, String])
      convert_for_cloud_search(value)
    else
      raise "Unsupported range type #{value.class}"
    end
  end

  def convert_for_prefix(value)
    case value
      when String
        value.gsub(/\*$/, '')
      else
       raise "Unsupported prefix type : #{value.class}"
    end
  end

  def normalize_rank(rank)
    rank = Array(rank)
    rank << :asc if rank.size < 2

    rank[1] == :desc ? "-#{rank[0]}" : rank[0]
  end

  def convert_date_or_time(obj)
    return obj unless [Time, Date, DateTime].include?(obj.class)
    obj.to_time.to_i
  end

end

Asari.mode = :sandbox # default to sandbox
