package cmd

import (
	v2 "dq/pkg/dq/v2"
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
	"github.com/spf13/cobra"
)

var queryCmd = &cobra.Command{
	Use:   "query",
	Short: "Query data quality",
	Run: func(cmd *cobra.Command, args []string) {
		if err := godotenv.Load(); err != nil {
			log.Fatal("Error loading .env file")
		}

		dsn := os.Getenv("DSN")

		spec, err := v2.ParseSpec(specPath)
		if err != nil {
			log.Fatalln(err)
		}

		compiler, err := v2.NewCompilerFromDSN(dsn)
		if err != nil {
			log.Fatalln(err)
		}

		executor := v2.NewExecutor(dsn, compiler)
		defer executor.Close()

		if err := executor.ConnectDB(); err != nil {
			log.Fatalln(err)
		}

		result, err := executor.Query(spec)
		if err != nil {
			log.Fatalln(err)
		}

		fmt.Println(result)
	},
}

var generateQueryCmd = &cobra.Command{
	Use:   "generate",
	Short: "Generate sql",
	Run: func(cmd *cobra.Command, args []string) {
		if err := godotenv.Load(); err != nil {
			log.Fatal("Error loading .env file")
		}

		dsn := os.Getenv("DSN")

		spec, err := v2.ParseSpec(specPath)
		if err != nil {
			log.Fatalln(err)
		}

		compiler, err := v2.NewCompilerFromDSN(dsn)
		if err != nil {
			log.Fatalln(err)
		}

		if returnSingleQuery {
			query, err := compiler.ToQuery(spec)
			if err != nil {
				log.Fatalln(err)
			}
			fmt.Println(query)
			return

		} else {
			statements, err := compiler.ToQueries(spec)
			if err != nil {
				log.Fatalln(err)
			}

			for idx, sql := range statements {
				fmt.Println(sql)
				if idx != len(statements)-1 {
					fmt.Println("=======")
				}
			}
			return
		}
	},
}

var specPath string
var format string
var returnSingleQuery bool

func init() {
	rootCmd.AddCommand(queryCmd)
	queryCmd.Flags().StringVarP(&specPath, "spec", "s", "dq.yaml", "Path to the rules file")
	queryCmd.Flags().StringVarP(&format, "format", "f", "plaintext", "Output format")
	_ = queryCmd.MarkFlagRequired("spec")

	rootCmd.AddCommand(generateQueryCmd)
	generateQueryCmd.Flags().StringVarP(&specPath, "spec", "s", "dq.yaml", "Path to the rules file")
	generateQueryCmd.Flags().BoolVar(&returnSingleQuery, "single-query", false, "Return a single query")
	_ = generateQueryCmd.MarkFlagRequired("spec")
}
