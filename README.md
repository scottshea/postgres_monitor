# PostgresMonitor

This gem is designed to expose a number of internal metrics and statistics for Postgres installations to the calling application. It borrows heavily from [Heroku's PG Extras](https://github.com/heroku/heroku-pg-extras) and [Boundless's plugin for New Relic](http://newrelic.com/plugins/boundless-production/109)-- [source](https://github.com/GoBoundless/newrelic_postgres_plugin).

In each case there were drawbacks that led me to create this gem. Heroku PG Extra's only works on Heroku installations. The New Relic plugin only accepts numerical values; I wanted to see the actual queries in some cases.

It uses the [PG gem](https://bitbucket.org/ged/ruby-pg/wiki/Home) for its connections to Postgres

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'postgres_monitor'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install postgres_monitor

## Usage

### Connection to the Database
To create a connection you will need to supply the host name, user, password (can be nil), and dbname. Port will default to 5432 if it is not provided. sslmode will default to required if it is not supplied.

#### Example

```ruby
@connection_params = {
  host: 'localhost',
  port: nil, # defaults to 5432
  user: 'postgres',
  password: 'password',
  sslmode: 'allow',
  dbname: 'postrges'
}

@monitor = PostgresMonitor::Monitor.new(@connection_params)
```

### Results from methods
The methods return the raw [PG::Result](http://deveiate.org/code/pg/PG/Result.html) for interpretation. This is a collection of hashes, well sometimes just one hash, with the results and keys in them.

#### Example
```ruby
results = @monitor.list_databases
databases = []
results.each do |row|
  databases << row['datname']
end
```

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake rspec` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/scottshea/postgres_monitor. This project is intended to be a safe, welcoming space for collaboration, and contributors are expected to adhere to the [Contributor Covenant](contributor-covenant.org) code of conduct.


## License

The gem is available as open source under the terms of the [MIT License](http://opensource.org/licenses/MIT).
