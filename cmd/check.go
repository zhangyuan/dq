package cmd

import (
	"dq/pkg/dq/executor"
	"log"
	"os"

	"github.com/joho/godotenv"
	"github.com/spf13/cobra"
)

var checkCmd = &cobra.Command{
	Use:   "check",
	Short: "Check data quality",
	Run: func(cmd *cobra.Command, args []string) {
		if err := godotenv.Load(); err != nil {
			log.Fatal("Error loading .env file")
		}

		passed, err := executor.Execute(specPath, format)
		if err != nil {
			log.Fatalln(err)
		}

		if !passed {
			os.Exit(1)
		}
	},
}

var specPath string
var format string

func init() {
	rootCmd.AddCommand(checkCmd)
	checkCmd.Flags().StringVarP(&specPath, "spec", "s", "dq.yaml", "Path to the rules file")
	checkCmd.Flags().StringVarP(&format, "format", "f", "plaintext", "Output format")
}
