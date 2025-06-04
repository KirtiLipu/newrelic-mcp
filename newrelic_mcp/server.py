import asyncio
import argparse
from mcp.server.fastmcp import FastMCP
import logging
from newrelic_mcp.client import NewRelicClient

# Initialize MCP at module level
mcp = FastMCP("newrelic-mcp")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('newrelic_mcp')

# Global variable for NewRelic client
nr_client = None


@mcp.tool()
async def get_newrelic_apm_metrics(application_name: str, time_range_minutes: int = 30) -> str:
    """
    Get Overall APM metrics from New Relic for a specific application.
    application_name: name of the application to get metrics for
    metric_names: list of metric names to get data for
        default metric_names: ['HttpDispatcher']
    metric_values: list of metric values to get data for
        default metric_values: ['average_response_time', 'calls_per_minute', 'call_count']
    transaction_name: name of the transaction to get details for
    time_range_minutes: time range in minutes to get data
    """
    metric_names = ['HttpDispatcher']
    metric_values = ['average_response_time', 'calls_per_minute', 'call_count']
    try:
        newrelic_application_id = await nr_client.find_newrelic_application_id(application_name)
        metrics_data = nr_client.get_app_metric_data(newrelic_application_id, metric_names, metric_values, time_range_minutes)
        return metrics_data
    except Exception as e:
        logger.error(f"Error fetching New Relic APM metrics: {str(e)}")
        return f"Error fetching New Relic APM metrics: {str(e)}"

@mcp.tool()
async def get_application_slow_transactions_details(application_name: str, time_range_minutes: int = 30):
    """
    Get the top N slow transactions and their breakdown segments at application level. 
    """
    app_id = await nr_client.find_newrelic_application_id(application_name)
    transactions_result = nr_client.get_slow_transactions(app_id, time_range_minutes)
    if "error" in transactions_result:
        return {"error": transactions_result["error"]}
    
    transactions = transactions_result.get("transactions", [])
    logger.info(f"Found {len(transactions)} transactions")
    
    combined = []
    for txn in transactions:
        txn_name = txn["name"]
        breakdown = get_transaction_breakdown_segments(app_id, txn_name, time_range_minutes)
        
        if "error" in breakdown:
            logger.warning(f"Failed to get breakdown for transaction {txn_name}: {breakdown['error']}")
            continue
            
        combined.append({
            "transaction": {
                "name": txn_name,
                "avg_duration": txn["avg_duration"],
                "min_duration": txn["min_duration"],
                "max_duration": txn["max_duration"],
                "call_count": txn["call_count"],
                "error_rate": txn["error_rate"],
                "throughput": txn["throughput"]
            },
            "breakdown": breakdown.get("segments", []),
            "total_duration_ms": breakdown.get("total_time_ms", 0)
        })

    return {
        "transactions": combined,
        "count": len(combined)
    }           

@mcp.tool()
async def get_application_top_database_operations_details(application_name: str, time_range_minutes: int = 30):
    """
    Get the top N database operations at application level.
    
    Returns a list of top N database operations with metrics like:
    {
        "datastoreType": "MySQL",         # Type of database (MySQL, PostgreSQL, Redis etc)
        "table": "milestone_milestoneconfig", # Name of the database table
        "operation": "select",            # SQL operation type (select, insert etc)
        "total_time_per_minute": 3398.21, # Total time spent per minute in ms
        "avg_query_time_ms": 3.78,        # Average query execution time in ms
        "throughput_ops_per_min": 899.63  # Number of operations per minute
    }
    """
    app_id = await nr_client.find_newrelic_application_id(application_name)
    application_top_database_operations = nr_client.get_top_database_operations(app_id, time_range_minutes)
    if "error" in application_top_database_operations:
        return {"error": application_top_database_operations["error"]}
    return {
        "database_operations": application_top_database_operations,
        "count": len(application_top_database_operations)
    }

@mcp.tool()
async def get_transaction_breakdown_segments(application_name: str, transaction_name: str, time_range_minutes: int = 30):
    """Get breakdown segments for a specific transaction or API endpoint uri using NRQL Metric table.

    Args:
        application_name: New Relic app name
        transaction_name: The name of the Transaction or API endpoint URI (e.g. '/api/v1/users' or 'WebTransaction/Controller/Home/index')
        time_range_minutes: Analysis window (1-1440 mins) default is 30 mins    

    Returns:
        {
            'transaction_name': str,      # Transaction name
            'total_time_ms': float,       # Total time
            'total_transaction_count': int,# Transaction count
            'segments': [{                # Performance segments
                'category': str,          # DB/External/Function
                'segment': str,           # Segment name
                'avg_time_ms': float,     # Avg time
                'percentage': float       # Time %
            }]
        }
    """
    app_id = await nr_client.find_newrelic_application_id(application_name)
        # Get total transaction count and transaction name
    transaction_total_query = f"""
    FROM Transaction
    SELECT latest(name) as 'transaction_name', count(*) as 'total_count'
    WHERE appId = {app_id}
    AND (name like '%{transaction_name}%' OR request.uri LIKE '%{transaction_name}%')
    SINCE {time_range_minutes} minutes ago
    """
    
    transaction_total_result = nr_client._make_insights_request(transaction_total_query)
    if not transaction_total_result or "error" in transaction_total_result:
        logger.error(f"Failed to fetch transaction count: {transaction_total_result.get('error', 'Unknown error')}")
        return {"error": "Failed to fetch transaction count"}

    # Extract transaction name and count from results
    results = transaction_total_result.get("results", [{}])
    if not results:
        logger.warning(f"No transaction data found for {transaction_name}")
        return {"error": "No transaction data found"}

    actual_transaction_name = results[0].get("latest")
    total_txn_count = results[1].get("count", 0)
    
    if total_txn_count == 0:
        logger.warning(f"No transactions found for {transaction_name}")
        total_txn_count = 1 

    # Main query to get segment details
    transaction_breakdown_query = f"""
    FROM Metric
    SELECT 
        average(convert(apm.service.transaction.overview, unit, 'ms')) AS 'avg_time',
        count(apm.service.transaction.overview) AS 'call_count',
        sum(convert(apm.service.transaction.overview, unit, 'ms')) AS 'total_time'
    WHERE (appId = {app_id}) 
        AND (transactionName = '{actual_transaction_name}' 
        OR transactionName IN (SELECT name FROM Transaction 
                             WHERE request.uri LIKE '%{transaction_name}%' LIMIT 1))
    FACET `metricTimesliceName`
    LIMIT 7
    SINCE {time_range_minutes} minutes ago 
    UNTIL now
    """
    logger.info(f"Transaction breakdown query: {transaction_breakdown_query}")
    print(transaction_breakdown_query)

    # Get segment breakdown
    transaction_breakdown_result = nr_client._make_insights_request(transaction_breakdown_query)
    logger.info(f"Transaction breakdown result: {transaction_breakdown_result}")

    if not transaction_breakdown_result or "error" in transaction_breakdown_result:
        logger.error(f"Failed to fetch transaction breakdown: {transaction_breakdown_result.get('error', 'Unknown error')}")
        return {"error": "Failed to fetch transaction breakdown"}

    breakdown_segments = []
    total_time = 0
    logger.info(f"Processing transaction breakdown for {actual_transaction_name}")

    for facet in transaction_breakdown_result.get("facets", []):
        segment_name = facet.get("name")
        results = facet.get("results", [])
        
        if not results or not segment_name:
            continue

        avg_time = float(results[0].get("average", 0))
        call_count = float(results[1].get("count", 0))
        segment_total_time = float(results[2].get("sum", 0))
        
        category = "Function"
        if segment_name.startswith("Datastore/"):
            category = "Database"
        elif segment_name.startswith("External/"):
            category = "External"
        
        avg_calls_per_txn = round(call_count / total_txn_count, 2)
        
        breakdown_segments.append({
            "category": category,
            "segment": segment_name,
            "avg_time_ms": round(avg_time, 2),
            "avg_calls_txn": avg_calls_per_txn,
            "total_time_ms": round(segment_total_time, 2),
            "percentage": 0
        })
        
        total_time += segment_total_time

    # Calculate percentages based on total time
    for segment in breakdown_segments:
        if total_time > 0:
            segment["percentage"] = round((segment["total_time_ms"] / total_time) * 100, 2)

    # Sort segments by percentage in descending order
    breakdown_segments.sort(key=lambda x: x["percentage"], reverse=True)

    return {
        "transaction_name": actual_transaction_name,
        "total_time_ms": round(total_time, 2),
        "total_transaction_count": total_txn_count,
        "segments": breakdown_segments
    }

async def run_server():
    """Run the MCP server with configuration from command line arguments"""
    global nr_client
    
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Run New Relic MCP server')
    parser.add_argument('--new-relic-api-key', required=True, help='New Relic API key')
    parser.add_argument('--nr-insights-api-key', required=True, help='New Relic Insights API key')
    parser.add_argument('--new-relic-account-id', required=True, help='New Relic account ID')
    parser.add_argument('--model', required=True, help='LLM model to use')
    
    # Parse arguments
    args = parser.parse_args()

    # Initialize New Relic client
    nr_client = NewRelicClient(
        args.new_relic_api_key,
        args.nr_insights_api_key,
        args.new_relic_account_id,
        args.model
    )
    
    # Initialize New Relic data
    await nr_client.initialize_newrelic()
    
    # Run the MCP server
    mcp.run(transport="stdio")

if __name__ == "__main__":
    asyncio.run(run_server())