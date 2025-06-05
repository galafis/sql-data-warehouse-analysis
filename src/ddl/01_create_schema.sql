-- Data Warehouse Schema Creation
-- Author: Gabriel Demetrios Lafis
-- Date: 2023-06-05
-- Description: Creates the schema for the Enterprise Data Warehouse

-- Create database if not exists
CREATE DATABASE IF NOT EXISTS enterprise_data_warehouse;

-- Use the database
USE enterprise_data_warehouse;

-- Create schema for staging area
CREATE SCHEMA IF NOT EXISTS staging;

-- Create schema for integration layer
CREATE SCHEMA IF NOT EXISTS integration;

-- Create schema for presentation layer (data marts)
CREATE SCHEMA IF NOT EXISTS sales_mart;
CREATE SCHEMA IF NOT EXISTS marketing_mart;
CREATE SCHEMA IF NOT EXISTS finance_mart;
CREATE SCHEMA IF NOT EXISTS hr_mart;
CREATE SCHEMA IF NOT EXISTS operations_mart;

-- Create schema for metadata
CREATE SCHEMA IF NOT EXISTS metadata;

-- Set default schema
SET search_path TO integration, public;

