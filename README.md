# AkasaAir Data Analysis System

A comprehensive data analysis system designed for understanding customer behavior and business performance at AkasaAir. The system provides robust analytical capabilities while maintaining ease of use.

## System Capabilities

The system functions as an enterprise-grade business analytics tool that:
- Processes customer data efficiently
- Identifies patterns in order history
- Provides actionable business insights
- Offers dual analysis approaches

## Analysis Approaches

### 1. Database Approach (`analyze_data.py`)
Optimal for scenarios involving:
- Large-scale data processing
- Regular analysis requirements
- Historical data retention
- Multi-user access requirements

### 2. In-Memory Approach (`analyze_data_inmemory.py`)
Suitable for scenarios requiring:
- High-performance analysis
- Ad-hoc insights generation
- Minimal setup requirements
- Immediate results delivery

## Implementation Guide

### Step 1: Environment Setup
System requirements and initial configuration:

1. **Prerequisites:**
   - Python 3.8 or higher (Core processing engine)
   - MySQL 8.0 or higher (Database management system)
   - pip (Package management tool)

2. **Package Installation:**
   ```powershell
   pip install -r requirements.txt
   ```

### Step 2: Database Configuration
Configure the environment settings file (`.env`):
```
DB_HOST=localhost
DB_USER=your_username
DB_PASSWORD=your_password
DB_NAME=AkasaAir
DB_PORT=3306
```

### Step 3: Data Preparation
Required data files:
- `task_DE_new_customers.csv` - Customer information dataset
- `task_DE_new_orders.xml` - Order transaction dataset

## Execution Instructions

### Database Implementation:
```powershell
python analyze_data.py
```

### In-Memory Implementation:
```powershell
python analyze_data_inmemory.py
```

## Analytics Overview

### 1. Customer Loyalty Analysis
- Customer retention metrics
- Order frequency tracking
- Current Lead: Rohan Gupta (4 orders)

### 2. Monthly Performance Metrics
- Month-over-month growth analysis
- Revenue trend monitoring
- Current Trend: Positive growth (September-November 2025)

### 3. Geographic Performance Analysis
- Regional revenue distribution
- Market opportunity identification
- Leading Region: West

### 4. Recent Customer Value Analysis
- Customer value assessment
- Spending pattern analysis
- Current Leader: Aarav Mehta (INR 38,247)

## Project Structure

```
AkasaAir/
├── analyze_data.py           # Database implementation
├── analyze_data_inmemory.py  # In-memory implementation
├── requirements.txt          # Dependency specifications
├── .env                      # Configuration settings
├── task_DE_new_customers.csv # Customer dataset
└── task_DE_new_orders.xml   # Order dataset
```

