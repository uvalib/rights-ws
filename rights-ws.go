package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/mux"
	"github.com/spf13/viper"
)

var db *sql.DB
var logger *log.Logger

const version = "1.4.1"

func main() {
	logger = log.New(os.Stdout, "", log.LstdFlags)

	// Load cfg
	logger.Printf("===> rights-ws staring up <===")
	logger.Printf("Load configuration...")
	viper.BindEnv("PORT")
	viper.BindEnv("DBHOST")
	viper.BindEnv("DBPORT")
	viper.BindEnv("DBNAME")
	viper.BindEnv("DBUSER")
	viper.BindEnv("DBPASS")
	viper.BindEnv("DB_OLD_PASSWDS")
	viper.BindEnv("DBTIMEOUT")

	logger.Printf("PORT           [%s]", viper.GetString("PORT"))
	logger.Printf("DBHOST         [%s]", viper.GetString("DBHOST"))
	logger.Printf("DBPORT         [%s]", viper.GetString("DBPORT"))
	logger.Printf("DBNAME         [%s]", viper.GetString("DBNAME"))
	logger.Printf("DBUSER         [%s]", viper.GetString("DBUSER"))
	logger.Printf("DBPASS         [%s]", strings.Repeat("*", len(viper.GetString("DBPASS"))))
	logger.Printf("DB_OLD_PASSWDS [%s]", viper.GetString("DB_OLD_PASSWDS"))
	logger.Printf("DBTIMEOUT      [%s]", viper.GetString("DBTIMEOUT"))

	// Init DB connection
	logger.Printf("Init DB connection...")
	connectStr := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?allowOldPasswords=%s&timeout=%ss&readTimeout=%ss&writeTimeout=%ss",
		viper.GetString("DBUSER"),
		viper.GetString("DBPASS"),
		viper.GetString("DBHOST"),
		viper.GetString("DBPORT"),
		viper.GetString("DBNAME"),
		viper.GetString("DB_OLD_PASSWDS"),
    	viper.GetString("DBTIMEOUT"),
		viper.GetString("DBTIMEOUT"),
		viper.GetString("DBTIMEOUT") )
	var err error
	db, err = sql.Open("mysql", connectStr)
	if err != nil {
		fmt.Printf("Database connection failed: %s", err.Error())
		os.Exit(1)
	}
	defer db.Close()

	if err = db.Ping(); err != nil {
		fmt.Printf("Database ping failed: %s", err.Error())
		os.Exit(1)
	}

	// Set routes and start server
	r := mux.NewRouter()
	r.HandleFunc("/", rootHandler)
	r.HandleFunc("/version", rootHandler)
	r.HandleFunc("/healthcheck", healthCheckHandler)
	r.HandleFunc("/{pid:.*}", rightsHandler)
	logger.Printf("Start service on port %s", viper.GetString("PORT"))
	http.ListenAndServe(":"+viper.GetString("PORT"), r)
}

/**
 * Handle a request for / or /version
 */
func rootHandler(rw http.ResponseWriter, req *http.Request) {
	logger.Printf("%s %s (%s)", req.Method, req.RequestURI, req.RemoteAddr)
	fmt.Fprintf(rw, "Access rights service version %s", version)
}

/**
 * Handle a request for /healthcheck
 */
func healthCheckHandler(rw http.ResponseWriter, req *http.Request) {
	logger.Printf("%s %s (%s)", req.Method, req.RequestURI, req.RemoteAddr)

	if err := db.Ping(); err != nil {
		logger.Printf("ERROR: pinging database (%s)", err.Error())
		rw.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(rw, "{\"database\":{\"healthy\":false,\"mesage\":\"%s\"}}", err.Error())
		return
	}
	rw.WriteHeader(http.StatusOK)
	fmt.Fprintf(rw, "{\"database\":{\"healthy\":true}}")
}

/**
 * Get rights statement for a PID
 */
func rightsHandler(rw http.ResponseWriter, req *http.Request) {
	logger.Printf("%s %s (%s)", req.Method, req.RequestURI, req.RemoteAddr)
	
	vars := mux.Vars(req)
	pid := vars["pid"]
	pidType, err := determinePidType(pid)
	if err != nil {
		rw.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(rw, "Error getting PID type for %s (%s)", pid, err.Error())
		return
	}

	if pidType == "metadata" {
		getMetadataRights(pid, rw)
		return
	}
	if pidType == "master_file" {
		getMasterFileRights(pid, rw)
		return
	}

	logger.Printf("WARNING: cant find: %s", pid)
	rw.WriteHeader(http.StatusNotFound)
	fmt.Fprintf(rw, "PID %s not found", pid)
}

func determinePidType(pid string) (string, error) {
	var cnt int
	qs := "select count(*) as cnt from metadata b where pid=?"
	err := db.QueryRow(qs, pid).Scan(&cnt)
	if err != nil {
		logger.Printf("ERROR: determining PID type for %s (%s)", pid, err.Error())
		return "", err
	}
	if cnt == 1 {
		return "metadata", nil
	}

	qs = "select count(*) as cnt from master_files b where pid=?"
	err = db.QueryRow(qs, pid).Scan(&cnt)
	if err != nil {
		logger.Printf("ERROR: determining PID type for %s (%s)", pid, err.Error())
		return "", err
	}
	if cnt == 1 {
		return "master_file", nil
	}

	return "invalid", nil
}

func getMetadataRights(pid string, rw http.ResponseWriter) {
	var policy sql.NullString
	qs := "select a.name from metadata b inner join availability_policies a on a.id=b.availability_policy_id where b.pid=?"
	err := db.QueryRow(qs, pid).Scan(&policy)
	if err != nil {
		logger.Printf("ERROR: getting metadata rights for %s (%s)", pid, err.Error())
		rw.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(rw, "Error getting metadata rights for %s (%s)", pid, err.Error())
		return
	}

	if policy.Valid {
		rw.WriteHeader(http.StatusOK)
		fmt.Fprintf(rw, "%s", strings.ToLower(strings.Split(policy.String, " ")[0]))
	} else {
		rw.WriteHeader(http.StatusOK)
		fmt.Fprint(rw, "private")
	}
}

func getMasterFileRights(pid string, rw http.ResponseWriter) {
	var policy sql.NullString
	qs :=
		`select a.name from master_files m
         inner join metadata b on b.id = m.metadata_id
         inner join availability_policies a on a.id = b.availability_policy_id
      where m.pid=?`
	err := db.QueryRow(qs, pid).Scan(&policy)
	if err != nil {
		logger.Printf("ERROR: getting master file rights for %s (%s)", pid, err.Error())
		rw.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(rw, "Error getting master file rights for %s (%s)", pid, err.Error())
		return
	}

	if policy.Valid {
		rw.WriteHeader(http.StatusOK)
		fmt.Fprintf(rw, "%s", strings.ToLower(strings.Split(policy.String, " ")[0]))
	} else {
		rw.WriteHeader(http.StatusOK)
		fmt.Fprint(rw, "private")
	}
}
