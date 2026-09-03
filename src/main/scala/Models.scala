case class Invoice(
  invoice_id: String,
  vendor_id: String,
  vendor_name: String,
  invoice_date: String,
  amount: Double,
  tax: Double,
  total_amount: Double,
  status: String
)
