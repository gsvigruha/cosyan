### Text functions

 * `concat(self: varchar, str: varchar): varchar`<br/>
   Concatenates `self` with `str`.

 * `contains(self: varchar, str: varchar): boolean`<br/>
   Returns true iff `self` contains `str`.

 * `index_of(self: varchar, str: varchar): integer`<br/>
   Index of the first occurrence of `str` in `self`.

 * `last_index_of(self: varchar, str: varchar): integer`<br/>
   Index of the last occurrence of `str` in `self`.

 * `length(self: varchar): integer`<br/>
   Number of characters in `self`.

 * `lower(self: varchar): varchar`<br/>
   Converts all characters of `self` to lowercase.

 * `matches(self: varchar, regex: varchar): boolean`<br/>
   Returns true iff `self` matches the regular expression `regex`.

 * `replace(self: varchar, target: varchar, replacement: varchar): varchar`<br/>
   Replaces every occurrences of `target` with `replacement` in `self`.

 * `substr(self: varchar, start: integer, end: integer): varchar`<br/>
   Returns the substring of `self` between `start` and `end`.

 * `trim(self: varchar): varchar`<br/>
   Removes all leading and trailing whitespaces from `self`.

 * `upper(self: varchar): varchar`<br/>
   Converts all characters of `self` to uppercase.

