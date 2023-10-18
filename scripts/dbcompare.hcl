job "cron:dbcompare" {
  datacenters = ["dc1"]
  type        = "batch"

  periodic {
    cron = "@weekly"
  }

  group "dbcompare" {
    count = 1

    task "dbcompare" {
      driver = "docker"
    
      config {
        image = "shinhwagk/tmp_dbcompare:3"
        volumes = ["local/log:/log"]
      }

      env {
        ARGS_SOURCE_DSN = "user/pass@1.1.1.1:3306"
        ARGS_TARGET_DSN = "user/pass@2.2.2.2:3306"
        ARGS_DATABASES = "db1,db2"
        ARGS_LOG_LOCATION = "/log"
      }
    }
  }
}
