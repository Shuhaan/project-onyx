resource "aws_cloudwatch_event_rule" "load_scheduler" {
  name                = "load_lambda_scheduler"
  description         = "Invoke load lambda function every 20 mins and upload results to CloudWatch"
  schedule_expression = "rate(20 minutes)"
  depends_on          = [aws_lambda_function.load_handler]
}

resource "aws_cloudwatch_event_target" "lambda_target" {
  rule      = aws_cloudwatch_event_rule.load_scheduler.name
  target_id = "load_lambda_target"
  arn       = aws_lambda_function.load_handler.arn
}

resource "aws_lambda_permission" "load_permission" {
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.load_handler.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.load_scheduler.arn
}