from flask import Flask, jsonify, render_template, request
import boto3
from botocore.exceptions import ClientError

app = Flask(__name__)

# Initialize DynamoDB client (configure your AWS credentials via AWS CLI or environment variables)
dynamodb = boto3.resource('dynamodb', region_name='ca-central-1')  # Canada Central region
TABLE_NAME = 'ExampleTable'

def create_dynamodb_table():
    """Create a DynamoDB table if it doesn't exist."""
    try:
        # Check if table already exists
        dynamodb.Table(TABLE_NAME).load()
        print(f"Table {TABLE_NAME} already exists.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            # Create table with 'id' as the partition key
            table = dynamodb.create_table(
                TableName=TABLE_NAME,
                KeySchema=[
                    {
                        'AttributeName': 'id',
                        'KeyType': 'HASH'  # Partition key
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'id',
                        'AttributeType': 'N'  # Number type for id
                    }
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5
                }
            )
            # Wait until the table is created
            table.wait_until_exists()
            print(f"Table {TABLE_NAME} created successfully.")
        else:
            raise e
    return dynamodb.Table(TABLE_NAME)

@app.route('/health')
def health():
    return "Up & Running"

@app.route('/create_table')
def create_table():
    try:
        create_dynamodb_table()
        return "Table created successfully or already exists"
    except Exception as e:
        return f"Error creating table: {str(e)}", 500

@app.route('/insert_record', methods=['POST'])
def insert_record():
    try:
        name = request.json['name']
        table = dynamodb.Table(TABLE_NAME)
        
        # Generate a unique ID (for simplicity, using a timestamp-based approach)
        import time
        item_id = int(time.time() * 1000)  # Milliseconds as a simple unique ID
        
        # Insert item into DynamoDB
        table.put_item(
            Item={
                'id': item_id,
                'name': name
            }
        )
        return "Record inserted successfully"
    except Exception as e:
        return f"Error inserting record: {str(e)}", 500

@app.route('/data')
def data():
    try:
        table = dynamodb.Table(TABLE_NAME)
        # Scan the table to retrieve all items (Note: Use query for production with indexes)
        response = table.scan()
        items = response['Items']
        # Convert DynamoDB decimal to JSON-serializable format
        for item in items:
            item['id'] = int(item['id'])  # Convert Decimal to int
        return jsonify(items)
    except Exception as e:
        return f"Error retrieving data: {str(e)}", 500

# UI route
@app.route('/')
def index():
    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')