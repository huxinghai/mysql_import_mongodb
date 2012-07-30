#encoding : utf-8
require "mysql"
require 'mongo'
require "json"

class MysqlImportMongodb
	MYSQL_OPTION = {
		:host => "localhost",
		:user_name => "root",
		:password => "",
		:database_name => "neza_development"
	}

	MONGO_OPTION = {
		:host => "localhost",
		:port => "27017"
	}

	def initialize(options = {})
		MYSQL_OPTION.merge!(options["mysql_option"]) unless options["mysql_option"].nil?
		MONGO_OPTION.merge!(options["mongo_option"]) unless options["mongo_option"].nil?
	end

	def execute()
		mysql_db = Mysql.init
		mysql_db.options(Mysql::SET_CHARSET_NAME,"utf8")
		mysql_db = Mysql.real_connect(MYSQL_OPTION[:host], MYSQL_OPTION[:user_name], MYSQL_OPTION[:password], MYSQL_OPTION[:database_name])
		mysql_db.query("SET NAMES utf8")

		mong_cn = Mongo::Connection.new(MONGO_OPTION[:host],MONGO_OPTION[:port])
		mong_db = mong_cn.db(MYSQL_OPTION[:database_name])

		mysql_tables = mysql_db.query("show tables")

		while tb_name = mysql_tables.fetch_row
			_tb_name = tb_name[0]
			sql = "select *from #{_tb_name}"
			table_infos = mysql_db.query(sql)
			while tb_row = table_infos.fetch_hash
				mong_db[_tb_name].insert(tb_row)
			end	
		end
		mysql_db.close
	end
end

m = MysqlImportMongodb.new
m.execute