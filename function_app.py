import azure.functions as func
import logging

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# HTTP Trigger function - Hello World example
@app.route(route="httpHelloWorld")
def httpHelloWorld(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )

# Random SQL Queries - Timer function
# Runs every 10 seconds and issues a random SQL query chosen from a list of examples to generate some load on the database.
@app.timer_trigger(schedule="*/10 * * * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def timerRandomSqlQueries(myTimer: func.TimerRequest) -> None:
    import os
    import time
    import random
    import pyodbc

    queries = [
                "select count(*) from FactResellerSales;",
                "select year(a.fulldatealternatekey), sum(b.salesamount) " +
                "  from dimdate a inner join FactResellerSales b " +
                "    on a.DateKey = b.orderdatekey " +
                " group by year(a.fulldatealternatekey) " +
                " order by 1 desc; ",
                "select d.EnglishProductCategoryName, c.EnglishProductSubcategoryName, year(b.FullDateAlternateKey), sum(a.SalesAmount)" +
                "  from FactResellerSales a " +
                "    inner join DimDate b " +
                "      on a.OrderDateKey = b.DateKey " +
                "	inner join DimProduct e " +
                "	  on a.ProductKey = e.ProductKey " +
                "	inner join DimProductSubcategory c " +
                "	  on e.ProductSubcategoryKey = c.ProductSubcategoryKey " +
                "	inner join DimProductCategory d " +
                "	  on c.ProductCategoryKey = d.ProductCategorykey " +
                "    group by d.EnglishProductCategoryName, c.EnglishProductSubcategoryName, year(b.FullDateAlternateKey) " +
                "	  having sum(a.SalesAmount) > 1000000 " +
                "	    order by sum(a.SalesAmount) desc;",
                "select top 10 b.ResellerName, sum(a.SalesAmount) as SalesAmount " +
                "  from FactResellerSales a " +
                "    inner join DimReseller b " +
                "	  on a.ResellerKey = b.ResellerKey " +
                "  group by b.ResellerName " +
                "  order by 2 desc;",
                "select b.EnglishDayNameOfWeek, min(b.DayNumberOfWeek) as [DayOfWeek], avg(a.Calls) as AvgCalls " +
                "  from FactCallCenter a " +
                "    inner join DimDate b " +
                "	  on a.DateKey = b.DateKey " +
                "  group by b.EnglishDayNameOfWeek " +
                "  order by 2;",
                "select c.EnglishCountryRegionName, sum(a.SalesAmount) as SalesAmount " +
                "  from FactInternetSales a " +
                "    inner join DimCustomer b " +
                "	  on a.CustomerKey = b.CustomerKey " +
                "	inner join DimGeography c " +
                "	  on b.GeographyKey = c.GeographyKey " +
                "    inner join DimDate d " +
                "	  on a.OrderDateKey = d.DateKey " +
                "  where d.CalendarYear = 2008 " +
                "  group by c.EnglishCountryRegionName " +
                "  order by 2 desc;",
                "with cte as " +
                "( " +
                "select d.EnglishProductCategoryName, c.EnglishProductSubcategoryName, year(b.FullDateAlternateKey) as Year, sum(a.SalesAmount) as SalesAmount " +
                "  from FactResellerSales a " + 
                "    inner join DimDate b " +
                "      on a.OrderDateKey = b.DateKey " +
                "	inner join DimProduct e " +
                "	  on a.ProductKey = e.ProductKey " +
                "	inner join DimProductSubcategory c " +
                "	  on e.ProductSubcategoryKey = c.ProductSubcategoryKey " +
                "	inner join DimProductCategory d " +
                "	  on c.ProductCategoryKey = d.ProductCategorykey " +
                "    group by d.EnglishProductCategoryName, c.EnglishProductSubcategoryName, year(b.FullDateAlternateKey) " +
                "	  having sum(a.SalesAmount) > 1000000 " +
                ") " +
                "SELECT * " +
                "FROM cte " +
                "PIVOT " +
                "( " +
                "   sum(SalesAmount) " +
                "   FOR [Year] IN ([2006],[2007],[2008]) " +
                ") " +
                "AS p " +
                "order by EnglishProductCategoryName, EnglishProductSubcategoryName; ",
                "select * " +
                "  from FactInternetSales " +
                "    where SalesAmount > 3500.00 and year(OrderDate) = 2006 ",
                "select SalesOrderNumber, SUM(ExtendedAmount) AS TotalSales " +
                "  from FactInternetSalesBig " +
                "    where SalesOrderNumber IN ('SO43705', 'SO43711', 'SO43714') " +
                "group by SalesOrderNumber ",
                "select * " +
                "  from FactInternetSalesBig " +
                "    where SalesOrderNumber IN ('SO54907', 'SO55062', 'SO55121') ",
                "select * " +
                "  from FactInternetSalesBig " +
                "    where SalesOrderNumber IN ('SO55339', 'SO55419', 'SO55428') ",        
                "select EnglishProductName, ProductSubcategoryKey, ReorderPoint, DaysToManufacture " +
                "  from DimProduct " +
                "    where ProductAlternateKey = 'AR-5381' ",
                "select EnglishProductName, ProductSubcategoryKey, ReorderPoint, DaysToManufacture " +
                "  from DimProduct " +
                "    where ProductAlternateKey = 'DT-2377' ",
                "select EnglishProductName, ProductSubcategoryKey, ReorderPoint, DaysToManufacture " +
                "  from DimProduct " +
                "    where ProductAlternateKey = 'RM-T801' ",
                "select EnglishProductName, ProductSubcategoryKey, ReorderPoint, DaysToManufacture " +
                "  from DimProduct " +
                "    where ProductAlternateKey = 'HL-U509-R' ",
                "select EnglishProductName, ProductSubcategoryKey, ReorderPoint, DaysToManufacture " +
                "  from DimProduct " +
                "    where ProductAlternateKey = 'FR-R38R-58' "
    ]

    try:
        # Get DB connection string
        sql_conn_str = os.getenv("db_conn_str")

        # Select a random query and the number of times to execute it
        queryIx = random.randint(0, len(queries) - 1)
        query = queries[queryIx]
        times = random.randint(1, 10)

        # Connect to the database and execute the query
        start_time = time.time_ns()

        with pyodbc.connect(sql_conn_str) as conn:
            cursor = conn.cursor()

            for i in range(times):
                cursor.execute(query)
                rows = cursor.fetchall()
        
            end_time = time.time_ns()

            timesStr = "time" if times == 1 else "times"

            logging.info(f"Executed query [{queryIx}] [{times}] {timesStr} returning [{len(rows) * times}] rows in [{(end_time - start_time) // 1000000}] milliseconds.")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
