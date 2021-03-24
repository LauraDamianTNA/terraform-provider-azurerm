package streamanalytics

import (
	"fmt"
	"log"
	"time"

	"github.com/Azure/azure-sdk-for-go/profiles/preview/preview/streamanalytics/mgmt/streamanalytics"
	"github.com/hashicorp/go-azure-helpers/response"
	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/helper/validation"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/helpers/azure"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/helpers/tf"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/internal/clients"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/internal/services/streamanalytics/parse"
	azSchema "github.com/terraform-providers/terraform-provider-azurerm/azurerm/internal/tf/schema"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/internal/timeouts"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/utils"
)

func resourceStreamAnalyticsReferenceInputSQL() *schema.Resource {
	return &schema.Resource{
		Create: resourceStreamAnalyticsReferenceInputSQLCreate,
		Read:   resourceStreamAnalyticsReferenceInputSQLRead,
		Update: resourceStreamAnalyticsReferenceInputSQLUpdate,
		Delete: resourceStreamAnalyticsReferenceInputSQLDelete,
		Importer: azSchema.ValidateResourceIDPriorToImport(func(id string) error {
			_, err := parse.StreamInputID(id)
			return err
		}),

		Timeouts: &schema.ResourceTimeout{
			Create: schema.DefaultTimeout(30 * time.Minute),
			Read:   schema.DefaultTimeout(5 * time.Minute),
			Update: schema.DefaultTimeout(30 * time.Minute),
			Delete: schema.DefaultTimeout(30 * time.Minute),
		},

		Schema: map[string]*schema.Schema{
			"name": {
				Type:         schema.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: validation.StringIsNotEmpty,
			},

			"stream_analytics_job_name": {
				Type:         schema.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: validation.StringIsNotEmpty,
			},

			"resource_group_name": azure.SchemaResourceGroupName(),

			"server": {
				Type:         schema.TypeString,
				Required:     true,
				ValidateFunc: validation.StringIsNotWhiteSpace,
			},

			"database": {
				Type:         schema.TypeString,
				Required:     true,
				ValidateFunc: validation.StringIsNotWhiteSpace,
			},

			"user": {
				Type:         schema.TypeString,
				Required:     true,
				ValidateFunc: validation.StringIsNotWhiteSpace,
			},

			"password": {
				Type:         schema.TypeString,
				Required:     true,
				Sensitive:    true,
				ValidateFunc: validation.StringIsNotWhiteSpace,
			},

			"table": {
				Type:         schema.TypeString,
				Optional:     true,
				ValidateFunc: validation.StringIsNotWhiteSpace,
			},

			"refresh_type": {
				Type:         schema.TypeString,
				Required:     true,
				ValidateFunc: validation.StringIsNotWhiteSpace,
			},

			"refresh_rate": {
				Type:         schema.TypeString,
				Required:     true,
				ValidateFunc: validation.StringIsNotWhiteSpace,
			},

			"full_snapshot_query": {
				Type:         schema.TypeString,
				Required:     true,
				ValidateFunc: validation.StringIsNotWhiteSpace,
			},

			"delta_snapshot_query": {
				Type:         schema.TypeString,
				Optional:     true,
				ValidateFunc: validation.StringIsNotWhiteSpace,
			},
		},
	}
}

func resourceStreamAnalyticsReferenceInputSQLCreate(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*clients.Client).StreamAnalytics.InputsClient
	subscriptionID := meta.(*clients.Client).Account.SubscriptionId
	ctx, cancel := timeouts.ForCreateUpdate(meta.(*clients.Client).StopContext, d)
	defer cancel()

	log.Printf("[INFO] preparing arguments for Azure Stream Analytics Reference Input SQL creation.")
	resourceID := parse.NewStreamInputID(subscriptionID, d.Get("resource_group_name").(string), d.Get("stream_analytics_job_name").(string), d.Get("name").(string))
	if d.IsNewResource() {
		existing, err := client.Get(ctx, resourceID.ResourceGroup, resourceID.StreamingjobName, resourceID.InputName)
		if err != nil {
			if !utils.ResponseWasNotFound(existing.Response) {
				return fmt.Errorf("checking for presence of existing %s: %+v", resourceID, err)
			}
		}

		if existing.ID != nil && *existing.ID != "" {
			return tf.ImportAsExistsError("azurerm_stream_analytics_reference_input_mssql", resourceID.ID())
		}
	}

	props, err := getSQLReferenceInputProps(d)
	if err != nil {
		return fmt.Errorf("creating the input props for resource creation: %v", err)
	}

	if _, err := client.CreateOrReplace(ctx, props, resourceID.ResourceGroup, resourceID.StreamingjobName, resourceID.InputName, "", ""); err != nil {
		return fmt.Errorf("creating %s: %+v", resourceID, err)
	}

	d.SetId(resourceID.ID())
	return resourceStreamAnalyticsReferenceInputSQLRead(d, meta)
}

func resourceStreamAnalyticsReferenceInputSQLUpdate(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*clients.Client).StreamAnalytics.InputsClient
	ctx, cancel := timeouts.ForCreateUpdate(meta.(*clients.Client).StopContext, d)
	defer cancel()

	log.Printf("[INFO] preparing arguments for Azure Stream Analytics Reference Input SQL update.")
	id, err := parse.StreamInputID(d.Id())
	if err != nil {
		return err
	}

	props, err := getSQLReferenceInputProps(d)
	if err != nil {
		return fmt.Errorf("creating the input props for resource update: %v", err)
	}

	if _, err := client.Update(ctx, props, id.ResourceGroup, id.StreamingjobName, id.InputName, ""); err != nil {
		return fmt.Errorf("updating %s: %+v", id, err)
	}

	return resourceStreamAnalyticsReferenceInputSQLRead(d, meta)
}

func resourceStreamAnalyticsReferenceInputSQLRead(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*clients.Client).StreamAnalytics.InputsClient
	ctx, cancel := timeouts.ForCreateUpdate(meta.(*clients.Client).StopContext, d)
	defer cancel()

	id, err := parse.StreamInputID(d.Id())
	if err != nil {
		return err
	}

	resp, err := client.Get(ctx, id.ResourceGroup, id.StreamingjobName, id.InputName)
	if err != nil {
		if utils.ResponseWasNotFound(resp.Response) {
			log.Printf("[DEBUG] %s was not found - removing from state!", id)
			d.SetId("")
			return nil
		}

		return fmt.Errorf("retrieving %s: %+v", id, err)
	}

	d.Set("name", id.InputName)
	d.Set("stream_analytics_job_name", id.StreamingjobName)
	d.Set("resource_group_name", id.ResourceGroup)

	if props := resp.Properties; props != nil {
		v, ok := props.AsReferenceInputProperties()
		if !ok {
			return fmt.Errorf("converting Reference Input SQL to a Reference Input: %+v", err)
		}

		sqlInputDataSource, ok := v.Datasource.AsAzureSQLReferenceInputDataSource()
		if !ok {
			return fmt.Errorf("converting Reference Input SQL to an SQL Stream Input: %+v", err)
		}

		d.Set("server", sqlInputDataSource.Properties.Server)
		d.Set("database", sqlInputDataSource.Properties.Database)
		d.Set("user", sqlInputDataSource.Properties.User)

		d.Set("table", sqlInputDataSource.Properties.Table)
		d.Set("refresh_type", sqlInputDataSource.Properties.RefreshType)
		d.Set("refresh_rate", sqlInputDataSource.Properties.RefreshRate)
		d.Set("full_snapshot_query", sqlInputDataSource.Properties.FullSnapshotQuery)
		d.Set("delta_snapshot_query", sqlInputDataSource.Properties.DeltaSnapshotQuery)
	}

	return nil
}

func resourceStreamAnalyticsReferenceInputSQLDelete(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*clients.Client).StreamAnalytics.InputsClient
	ctx, cancel := timeouts.ForCreateUpdate(meta.(*clients.Client).StopContext, d)
	defer cancel()

	id, err := parse.StreamInputID(d.Id())
	if err != nil {
		return err
	}

	if resp, err := client.Delete(ctx, id.ResourceGroup, id.StreamingjobName, id.InputName); err != nil {
		if !response.WasNotFound(resp.Response) {
			return fmt.Errorf("deleting %s: %+v", id, err)
		}
	}

	return nil
}

func optionalString(s interface{}, defaultValue *string) *string {
	if s == nil {
		return defaultValue
	}
	retval := s.(string)
	if len(retval) == 0 {
		return defaultValue
	}
	return &retval
}

func getSQLReferenceInputProps(d *schema.ResourceData) (streamanalytics.Input, error) {
	name := d.Get("name").(string)
	server := d.Get("server").(string)
	database := d.Get("database").(string)
	user := d.Get("user").(string)
	password := d.Get("password").(string)
	fullSnapshotQuery := d.Get("full_snapshot_query").(string)
	table := optionalString(d.Get("table"), nil)
	refreshTypeDefault := "Static"
	refreshType := optionalString(d.Get("refresh_type"), &refreshTypeDefault)
	refreshRateDefault := "00:00:00"
	refreshRate := optionalString(d.Get("refresh_rate"), &refreshRateDefault)
	deltaSnapshotQuery := optionalString(d.Get("delta_snapshot_query"), nil)

	props := streamanalytics.Input{
		Name: utils.String(name),
		Properties: &streamanalytics.ReferenceInputProperties{
			Type: streamanalytics.TypeReference,
			Datasource: &streamanalytics.AzureSQLReferenceInputDataSource{
				Type: streamanalytics.TypeBasicReferenceInputDataSourceTypeMicrosoftSQLServerDatabase,
				Properties: &streamanalytics.AzureSQLReferenceInputDataSourceProperties{
					Server:             utils.String(server),
					Database:           utils.String(database),
					User:               utils.String(user),
					Password:           utils.String(password),
					Table:              table,
					RefreshType:        refreshType,
					RefreshRate:        refreshRate,
					FullSnapshotQuery:  utils.String(fullSnapshotQuery),
					DeltaSnapshotQuery: deltaSnapshotQuery,
				},
			},
		},
	}

	return props, nil
}
