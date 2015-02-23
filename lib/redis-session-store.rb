require 'redis'
require 'oj'

# Redis session storage for Rails, and for Rails only. Derived from
# the MemCacheStore code, simply dropping in Redis instead.
#
# Options:
#  :key     => Same as with the other cookie stores, key name
#  :secret  => Encryption secret for the key
#  :redis => {
#    :host    => Redis host name, default is localhost
#    :port    => Redis port, default is 6379
#    :db      => Database number, defaults to 0. Useful to separate your session storage from other data
#    :key_prefix  => Prefix for keys used in Redis, e.g. myapp-. Useful to separate session storage keys visibly from others
#    :expire_after => A number in seconds to set the timeout interval for the session. Will map directly to expiry in Redis
#  }
#  :serializer => Serializer to use on session data, default is :marshal
class RedisSessionStore < ActionController::Session::AbstractStore

  # Uses built-in JSON library to encode/decode session
  class JsonSerializer
    def self.load(value)
      Oj.load(value)
    end

    def self.dump(value)
      Oj.dump(value, :mode => :object)
    end
  end

  def initialize(app, options = {})
    super

    redis_options = options[:redis] || {}

    @default_options.merge!(:namespace => 'rack:session')
    @default_options.merge!(redis_options)
    @redis = Redis.new(redis_options)
    @serializer = determine_serializer(options[:serializer])
  end

  private
    def prefixed(sid)
      "#{@default_options[:key_prefix]}#{sid}"
    end

    def generate_sid
      loop do
        sid = super
        break sid unless @redis.get(prefixed(sid))
      end
    end

    def get_session(env, sid)
      unless sid && (session = load_session_from_redis(sid))
        sid = generate_sid
        session = {}
      end

      [sid, session]
    rescue Errno::ECONNREFUSED
      [generate_sid, {}]
    end

    def load_session_from_redis(sid)
      data = @redis.get(prefixed(sid))

      data ? decode(data) : nil
    end

    def decode(data)
      @serializer.load(data)
    end

    def set_session(env, sid, session_data)
      options = env['rack.session.options']
      expiry  = options[:expire_after] || nil
      if expiry
        @redis.setex(prefixed(sid), expiry, encode(session_data))
      else
        @redis.set(prefixed(sid), encode(session_data))
      end
      return true
    rescue Errno::ECONNREFUSED
      return false
    end

    def encode(session_data)
      @serializer.dump(session_data)
    end

    def destroy(env)
      if env['rack.request.cookie_hash'] && env['rack.request.cookie_hash'][@key]
        @redis.del( prefixed(env['rack.request.cookie_hash'][@key]) )
      end
    rescue Errno::ECONNREFUSED
      Rails.logger.warn("RedisSessionStore#destroy: Connection to redis refused")
    end

    def determine_serializer(serializer)
      serializer ||= :marshal
      case serializer
      when :marshal then Marshal
      when :json then JsonSerializer
      else serializer
      end
    end
end
