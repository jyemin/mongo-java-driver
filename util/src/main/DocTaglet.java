/*
 * Copyright 20017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import com.sun.source.doctree.DocTree;
import jdk.javadoc.doclet.Taglet;

import javax.lang.model.element.Element;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class DocTaglet implements Taglet {

    @Override
    public Set<Location> getAllowedLocations() {
        return new HashSet<Location>(Arrays.asList(
                Location.CONSTRUCTOR,
                Location.METHOD,
                Location.FIELD,
                Location.OVERVIEW,
                Location.PACKAGE,
                Location.TYPE));
    }

    public boolean isInlineTag() {
        return false;
    }

    @Override
    public String toString(final List<? extends DocTree> docTreeList, final Element ignored) {
        StringBuilder buf = new StringBuilder();
        buf.append("<dl>\n");
        buf.append(String.format("   <dt><span class=\"strong\">%s</span></dt>\n", getHeader()));
        for (DocTree t : docTreeList) {
            buf.append("   <dd>").append(genLink(t.toString())).append("</dd>\n");
        }
        buf.append("</dl>\n");
        return buf.toString();
    }

    protected abstract String getHeader();

    protected abstract String getBaseDocURI();

    // Generate link from strings like:
    //    @mongodb.server.release 3.6
    //    @mongodb.driver.manual reference/mongodb-extended-json/ MongoDB Extended JSON
    private String genLink(final String text) {
        String relativePath;
        String display;

        int firstSpace = text.indexOf(' ');
        int secondSpace = text.indexOf(' ', firstSpace + 1);
        if (secondSpace != -1 && secondSpace != firstSpace) {
            relativePath = text.substring(firstSpace, secondSpace).trim();
            display = text.substring(secondSpace, text.length()).trim();
        } else {
            relativePath = text.substring(firstSpace).trim();
            display = relativePath;
        }

        return String.format("<a href='%s%s'>%s</a>", getBaseDocURI(), relativePath, display);
    }
}
