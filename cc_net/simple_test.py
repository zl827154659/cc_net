doc: dict = {
    "language": "zh",
    "raw_content": "12345"
}
lang_whitelist = ["zh"]
fun1=lambda doc1: doc1.get("language") in lang_whitelist
print(fun1(doc))
