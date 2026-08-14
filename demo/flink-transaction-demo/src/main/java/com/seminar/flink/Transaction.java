package com.seminar.flink;

public class Transaction {

    public String transaction_id;
    public String account_id;
    public String merchant_id;
    public double amount;
    public String event_time;

    public Transaction() {
        // Jackson cần constructor rỗng để parse JSON
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "transaction_id='" + transaction_id + '\'' +
                ", account_id='" + account_id + '\'' +
                ", merchant_id='" + merchant_id + '\'' +
                ", amount=" + amount +
                ", event_time='" + event_time + '\'' +
                '}';
    }
}