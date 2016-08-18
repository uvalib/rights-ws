package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/julienschmidt/httprouter"
	"github.com/spf13/viper"
)

var db *sql.DB
var logger *log.Logger

const version = "1.1.0"

func main() {
	lf, _ := os.OpenFile("service.log", os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	defer lf.Close()
	logger = log.New(lf, "service: ", log.LstdFlags)
	// use below to log to console....
	//logger = log.New(os.Stdout, "logger: ", log.LstdFlags)

	// Load cfg
	logger.Printf("===> rights-ws staring up <===")
	logger.Printf("Load configuration...")
	viper.SetConfigName("config")
	viper.SetConfigType("yml")
	viper.AddConfigPath(".")
	err := viper.ReadInConfig()
	if err != nil {
		fmt.Printf("Unable to read config: %s", err.Error())
		os.Exit(1)
	}

	// Init DB connection
	logger.Printf("Init DB connection...")
	connectStr := fmt.Sprintf("%s:%s@tcp(%s)/%s?allowOldPasswords=%s", viper.GetString("db_user"), viper.GetString("db_pass"),
		viper.GetString("db_host"), viper.GetString("db_name"), viper.GetString("db_old_passwords"))
	db, err = sql.Open("mysql", connectStr)
	if err != nil {
		fmt.Printf("Database connection failed: %s", err.Error())
		os.Exit(1)
	}
	defer db.Close()

	// Set routes and start server
	mux := httprouter.New()
	mux.GET("/", rootHandler)
	mux.GET("/:pid", rightsHandler)
	logger.Printf("Start service on port %s", viper.GetString("port"))
	http.ListenAndServe(":"+viper.GetString("port"), mux)
}

/**
 * Handle a request for /
 */
func rootHandler(rw http.ResponseWriter, req *http.Request, params httprouter.Params) {
	logger.Printf("%s %s", req.Method, req.RequestURI)
	fmt.Fprintf(rw, "Access rights service version %s", version)
}

/**
 * Get rights statement for a PID
 */
func rightsHandler(rw http.ResponseWriter, req *http.Request, params httprouter.Params) {
	logger.Printf("%s %s", req.Method, req.RequestURI)
	pid := params.ByName("pid")
	pidType := determinePidType(pid)
	if pidType == "metadata" {
		getMetadataRights(pid, rw)
	} else if pidType == "master_file" {
		getMasterFileRights(pid, rw)
	} else {
		logger.Printf("Couldn't find %s", pid)
		rw.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(rw, "PID %s not found", pid)
	}
}

func determinePidType(pid string) (pidType string) {
	var cnt int
	pidType = "invalid"
	qs := "select count(*) as cnt from metadata b where pid=?"
	db.QueryRow(qs, pid).Scan(&cnt)
	if cnt == 1 {
		pidType = "metadata"
		return
	}

	qs = "select count(*) as cnt from master_files b where pid=?"
	db.QueryRow(qs, pid).Scan(&cnt)
	if cnt == 1 {
		pidType = "master_file"
		return
	}

	return
}

func getMetadataRights(pid string, rw http.ResponseWriter) {
	var policy sql.NullString
	qs := "select a.name from metadata b inner join availability_policies a on a.id=b.availability_policy_id where b.pid=?"
	db.QueryRow(qs, pid).Scan(&policy)
	if policy.Valid {
		fmt.Fprintf(rw, "%s", strings.ToLower(strings.Split(policy.String, " ")[0]))
	} else {
		fmt.Fprint(rw, "private")
	}
}

func getMasterFileRights(pid string, rw http.ResponseWriter) {
	var policy sql.NullString
	qs :=
		`select a.name from master_files m
         inner join units u on u.id = m.unit_id
         inner join metadata b on b.id = u.metadata_id
         inner join availability_policies a on a.id = b.availability_policy_id
      where m.pid=?`
	db.QueryRow(qs, pid).Scan(&policy)
	if policy.Valid {
		fmt.Fprintf(rw, "%s", strings.ToLower(strings.Split(policy.String, " ")[0]))
	} else {
		fmt.Fprint(rw, "private")
	}
}
