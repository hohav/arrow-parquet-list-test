use std::{error::Error, fs::File, sync::Arc};

use ::arrow::{
	array::{ArrayRef, Int32Builder, ListBuilder},
	record_batch::RecordBatch,
};

use parquet::arrow::arrow_writer::ArrowWriter;

pub fn main() -> Result<(), Box<dyn Error>> {
	let mut builder = ListBuilder::new(Int32Builder::new(10));
	builder.values().append_value(1)?;
	builder.append(true)?;
	builder.values().append_value(2)?;
	builder.append(true)?;

	let values = builder.finish();

	let batch = RecordBatch::try_from_iter_with_nullable(
		vec![("values", Arc::new(values) as ArrayRef, false)]
	)?;

	let buf = File::create("test.parquet")?;
	let mut writer = ArrowWriter::try_new(buf, batch.schema(), None)?;
	writer.write(&batch)?;
	writer.close()?;

	Ok(())
}
