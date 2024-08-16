resource "aws_cloudwatch_event_rule" "extract_scheduler" {
  name                = "extract_lambda_scheduler"
  description         = "Invoke extract lambda function every 5 mins and upload results to CloudWatch"
  schedule_expression = "rate(10 minutes)"
  depends_on          = [aws_lambda_function.extract_handler]
}

resource "aws_cloudwatch_event_target" "lambda_target" {
  rule                = aws_cloudwatch_event_rule.extract_scheduler.name
  target_id           = "extract_lambda_target"
  arn                 = aws_lambda_function.extract_handler.arn
}

resource "aws_lambda_permission" "extract_permission" {
  action              = "lambda:InvokeFunction"
  function_name       = aws_lambda_function.extract_handler.function_name
  principal           = "events.amazonaws.com"
  source_arn          = aws_cloudwatch_event_rule.extract_scheduler.arn
}