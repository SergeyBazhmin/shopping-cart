#!/bin/bash

kcat -b localhost:9092 -t cart-events  -T -P -l test_data.jsonlines