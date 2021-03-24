package streamanalytics_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/terraform"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/internal/acceptance"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/internal/acceptance/check"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/internal/clients"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/utils"
)

type StreamAnalyticsReferenceInputSQLResource struct{}

func TestAccStreamAnalyticsReferenceInputSQL_create(t *testing.T) {
	data := acceptance.BuildTestData(t, "azurerm_stream_analytics_reference_input_mssql", "test")
	r := StreamAnalyticsReferenceInputSQLResource{}

	data.ResourceTest(t, r, []resource.TestStep{
		{
			Config: r.created(data),
			Check: resource.ComposeTestCheckFunc(
				check.That(data.ResourceName).ExistsInAzure(r),
			),
		},
		data.ImportStep("password"),
	})
}

func TestAccStreamAnalyticsReferenceInputSQL_update(t *testing.T) {
	data := acceptance.BuildTestData(t, "azurerm_stream_analytics_reference_input_mssql", "test")
	r := StreamAnalyticsReferenceInputSQLResource{}

	data.ResourceTest(t, r, []resource.TestStep{
		{
			Config: r.created(data),
			Check: resource.ComposeTestCheckFunc(
				check.That(data.ResourceName).ExistsInAzure(r),
			),
		},
		{
			Config: r.updated(data),
			Check: resource.ComposeTestCheckFunc(
				check.That(data.ResourceName).ExistsInAzure(r),
			),
		},
		data.ImportStep("password"),
	})
}

func (r StreamAnalyticsReferenceInputSQLResource) Exists(ctx context.Context, client *clients.Client, state *terraform.InstanceState) (*bool, error) {
	name := state.Attributes["name"]
	jobName := state.Attributes["stream_analytics_job_name"]
	resourceGroup := state.Attributes["resource_group_name"]

	resp, err := client.StreamAnalytics.InputsClient.Get(ctx, resourceGroup, jobName, name)
	if err != nil {
		if utils.ResponseWasNotFound(resp.Response) {
			return utils.Bool(false), nil
		}
		return nil, fmt.Errorf("retrieving Stream Output %q (Stream Analytics Job %q / Resource Group %q): %+v", name, jobName, resourceGroup, err)
	}
	return utils.Bool(true), nil
}

func (r StreamAnalyticsReferenceInputSQLResource) created(data acceptance.TestData) string {
	template := r.template(data)
	return fmt.Sprintf(`
%s

resource "azurerm_stream_analytics_reference_input_mssql" "test" {
  name                      = "acctestinput-%d"
  stream_analytics_job_name = azurerm_stream_analytics_job.test.name
  resource_group_name       = azurerm_stream_analytics_job.test.resource_group_name

  server = azurerm_mssql_server.test.name
  user = azurerm_mssql_server.test.administrator_login
  password = azurerm_mssql_server.test.administrator_login_password
  database = azurerm_sql_database.test.name 
  refresh_type              = "Static"
  refresh_rate              = "00:00:00"
  full_snapshot_query = <<QUERY
    SELECT *
    FROM [YourInputTable]
QUERY

}
`, template, data.RandomInteger)
}

func (r StreamAnalyticsReferenceInputSQLResource) updated(data acceptance.TestData) string {
	template := r.template(data)
	return fmt.Sprintf(`
%s

resource "azurerm_mssql_server" "updated" {
  name                         = "acctest-sqlserver-updated-%[2]d"
  resource_group_name          = azurerm_resource_group.test.name
  location                     = azurerm_resource_group.test.location
  version                      = "12.0"
  administrator_login          = "mradministrator"
  administrator_login_password = "thisIsDog12"
}

resource "azurerm_sql_database" "updated" {
  name      				  = "acctest-db-updated-%[2]d"
  server_name         = azurerm_mssql_server.test.name
  location            = "%[3]s"
  resource_group_name = azurerm_resource_group.test.name
}

resource "azurerm_stream_analytics_reference_input_mssql" "test" {
  name                      = "acctestinput-%[2]d"
  stream_analytics_job_name = azurerm_stream_analytics_job.test.name
  resource_group_name       = azurerm_stream_analytics_job.test.resource_group_name

  server = azurerm_mssql_server.updated.name
  database = azurerm_sql_database.updated.name 
  user = azurerm_mssql_server.updated.administrator_login
  password = azurerm_mssql_server.updated.administrator_login_password
  refresh_type              = "Static"
  refresh_rate              = "00:00:00"
  full_snapshot_query = <<QUERY
    SELECT *
    FROM [YourInputTable]
QUERY

}
`, template, data.RandomInteger, data.Locations.Primary)
}

func (r StreamAnalyticsReferenceInputSQLResource) template(data acceptance.TestData) string {
	return fmt.Sprintf(`
provider "azurerm" {
  features {}
}

resource "azurerm_resource_group" "test" {
  name     = "acctestRG-%d"
  location = "%s"
}

resource "azurerm_mssql_server" "test" {
  name                         = "acctest-sqlserver-%[1]d"
  resource_group_name          = azurerm_resource_group.test.name
  location                     = azurerm_resource_group.test.location
  version                      = "12.0"
  administrator_login          = "mradministrator"
  administrator_login_password = "thisIsDog11"
}

resource "azurerm_sql_database" "test" {
  name      				  = "acctest-db-%[1]d"
  server_name         = azurerm_mssql_server.test.name
  location            = "%[2]s"
  resource_group_name = azurerm_resource_group.test.name
}


resource "azurerm_stream_analytics_job" "test" {
  name                                     = "acctestjob-%[1]d"
  resource_group_name                      = azurerm_resource_group.test.name
  location                                 = azurerm_resource_group.test.location
  compatibility_level                      = "1.0"
  data_locale                              = "en-GB"
  events_late_arrival_max_delay_in_seconds = 60
  events_out_of_order_max_delay_in_seconds = 50
  events_out_of_order_policy               = "Adjust"
  output_error_policy                      = "Drop"
  streaming_units                          = 1

  transformation_query = <<QUERY
    SELECT *
    INTO [YourOutputAlias]
    FROM [YourInputAlias]
QUERY
}
`, data.RandomInteger, data.Locations.Primary)
}
