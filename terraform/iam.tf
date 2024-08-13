# ---------------
# Lambda IAM Role
# ---------------

# Define
data "aws_iam_policy_document" "trust_policy" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

# Create
resource "aws_iam_role" "extract_lambda_role" {
  name_prefix        = "role-${var.extract_lambda}"
  assume_role_policy = data.aws_iam_policy_document.trust_policy.json
}


# ------------------------------
# Lambda IAM Policy for S3 Write
# ------------------------------

# Define

data "aws_iam_policy_document" "s3_data_policy_doc" {
  statement {
    actions = ["s3:PutObject"]
    resources = [aws_s3_bucket.ingested_data_bucket.arn]
    effect = "Allow"
  }
}

# Create
resource "aws_iam_policy" "s3_write_policy" {
  name_prefix = "s3-policy-${var.extract_lambda}-write"
  policy = data.aws_iam_policy_document.s3_data_policy_doc.json
}

# Attach
resource "aws_iam_role_policy_attachment" "lambda_s3_write_policy_attachment" {
  role = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.s3_write_policy.arn
}


# ------------------------------
# Lambda IAM Policy for CloudWatch
# ------------------------------

# Define
data "aws_iam_policy_document" "cw_document" {
  statement {
    actions = [ 
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = ["*"]  

    effect = "Allow"
  }

  statement {
    actions   = [
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.extract_lambda}:*"
    ]
    effect    = "Allow"
  }
}

# Create
resource "aws_iam_policy" "cw_policy" {
  name_prefix = "cw-policy-${var.extract_lambda}"
  policy      = data.aws_iam_policy_document.cw_document.json
}


# Attach
resource "aws_iam_role_policy_attachment" "lambda_cw_policy_attachment" {
  role       = aws_iam_role.lambda_role.name
  policy_arn  = aws_iam_policy.cw_policy.arn
}

