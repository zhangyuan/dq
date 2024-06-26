package cmd

import (
	v2 "dq/pkg/dq/v2"
	"dq/pkg/dq/v2/adapters"
	"dq/pkg/dq/v2/helpers"
	"dq/pkg/dq/v2/report"
	"encoding/json"
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

		adapter, err := adapters.NewAdapterFromDSN(driver, dsn)
		if err != nil {
			log.Fatalln(err)
		}

		compiler := v2.NewCompiler(adapter)
		if err != nil {
			log.Fatalln(err)
		}

		params, err := GetParams(paramsPath)
		if err != nil {
			log.Fatalln(err)
		}

		executor := v2.NewExecutor(adapter, compiler)
		defer executor.Close()

		if err := executor.ConnectDB(); err != nil {
			log.Fatalln(err)
		}

		result, err := executor.Query(spec, params)
		if err != nil {
			log.Fatalln(err)
		}

		if format == "table" {
			report := report.NewTable(result)
			fmt.Println(report.Render())
		} else if format == "json" {
			bytes, err := json.MarshalIndent(result, "", "  ")
			if err != nil {
				log.Fatalln(err)
			}
			fmt.Println(string(bytes))
		}

		if !result.IsOk {
			os.Exit(1)
		}
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

		adapter, err := adapters.NewAdapterFromDSN(driver, dsn)
		if err != nil {
			log.Fatalln(err)
		}

		compiler := v2.NewCompiler(adapter)
		if err != nil {
			log.Fatalln(err)
		}

		params, err := GetParams(paramsPath)
		if err != nil {
			log.Fatalln(err)
		}

		query, err := compiler.ToQuery(spec, params)
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Println(query)
	},
}

func GetParams(path string) (*map[string]any, error) {
	if paramsPath != "" {
		return helpers.ParseYAMLFromFile[map[string]any](paramsPath)
	} else {
		return &map[string]any{}, nil
	}
}

var generateQueriesCmd = &cobra.Command{
	Use:   "generate-queries",
	Short: "Generate sql queries",
	Run: func(cmd *cobra.Command, args []string) {
		if err := godotenv.Load(); err != nil {
			log.Fatal("Error loading .env file")
		}

		dsn := os.Getenv("DSN")

		spec, err := v2.ParseSpec(specPath)
		if err != nil {
			log.Fatalln(err)
		}

		adapter, err := adapters.NewAdapterFromDSN(driver, dsn)
		if err != nil {
			log.Fatalln(err)
		}

		compiler := v2.NewCompiler(adapter)
		if err != nil {
			log.Fatalln(err)
		}

		params, err := GetParams(paramsPath)
		if err != nil {
			log.Fatalln(err)
		}

		statements, err := compiler.ToQueries(spec, params)
		if err != nil {
			log.Fatalln(err)
		}

		for idx, sql := range statements {
			fmt.Println(sql)
			if idx != len(statements)-1 {
				fmt.Println("----------")
			}
		}
	},
}

var specPath string
var format string
var paramsPath string
var driver string

func init() {
	rootCmd.AddCommand(queryCmd)
	queryCmd.Flags().StringVarP(&specPath, "spec", "s", "dq.yaml", "Path to the rules file")
	queryCmd.Flags().StringVarP(&format, "format", "f", "table", "Output format: table, json")
	queryCmd.Flags().StringVar(&paramsPath, "params-path", "", "Path to params file")
	queryCmd.Flags().StringVar(&driver, "driver", "", "Database driver. e.g. odps, postgres")
	_ = queryCmd.MarkFlagRequired("spec")
	_ = queryCmd.MarkFlagRequired("driver")

	rootCmd.AddCommand(generateQueryCmd)
	generateQueryCmd.Flags().StringVarP(&specPath, "spec", "s", "dq.yaml", "Path to the rules file")
	generateQueryCmd.Flags().StringVar(&paramsPath, "params-path", "", "Path to params file")
	generateQueryCmd.Flags().StringVar(&driver, "driver", "", "Database driver. e.g. odps, postgres")
	_ = generateQueryCmd.MarkFlagRequired("spec")
	_ = generateQueryCmd.MarkFlagRequired("driver")

	rootCmd.AddCommand(generateQueriesCmd)
	generateQueriesCmd.Flags().StringVarP(&specPath, "spec", "s", "dq.yaml", "Path to the rules file")
	generateQueriesCmd.Flags().StringVar(&paramsPath, "params-path", "", "Path to params file")
	generateQueriesCmd.Flags().StringVar(&driver, "driver", "", "Database driver. e.g. odps, postgres")
	_ = generateQueriesCmd.MarkFlagRequired("spec")
	_ = generateQueriesCmd.MarkFlagRequired("driver")
}
