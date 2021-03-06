# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'postgres_monitor/version'

Gem::Specification.new do |spec|
  spec.name          = 'postgres_monitor'
  spec.version       = PostgresMonitor::VERSION
  spec.authors       = ['Scott Shea']
  spec.email         = ['scott.j.shea@gmail.com']

  spec.summary       = %q{Gem to help monitor Postgres Instances}
  spec.description   = %q{This gem is designed to open up SQL queries programatically so that Postgres Databases can be easily monitored}
  spec.homepage      = 'https://github.com/scottshea/postgres_monitor'
  spec.license       = 'MIT'

  spec.files         = `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  spec.bindir        = 'exe'
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ['lib']

  spec.add_dependency('pg', '~> 0.17', '>= 0.17.0')

  spec.add_development_dependency 'bundler', '~> 1.10'
  spec.add_development_dependency 'rake', '~> 10.0'
  spec.add_development_dependency 'rspec'
end
