package xyz.mfj;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;

import org.apache.commons.cli.ParseException;
import org.junit.jupiter.api.Test;

public class AppTest 
{
    @Test
    public void errorInParse() {
        ArrayList<String[]> cmds = new ArrayList<>();
        String[] syntaxMisMatch = new String[] {
            "-hc"
        };
        cmds.add(syntaxMisMatch);
        String[] noArgument = new String[] {
            "--prepare-dat"
        };
        cmds.add(noArgument);
        String[] anotherSyntaxMisMatch = new String[] {
            "--prepare-dat abc"
        };
        cmds.add(anotherSyntaxMisMatch);
        String[] mixedSyntax = new String[] {
            "-hc",
            "-p=yttr-2022"
        };
        cmds.add(mixedSyntax);

        boolean allFailed = true;
        for (String[] cmd : cmds) {
            try {
                App.main(cmd);
                allFailed = false;
            } catch (Exception e) {
                assertEquals(ParseException.class, e.getClass().getSuperclass());
                System.out.println(((ParseException)e).getMessage());
            } 
        }
        assertTrue(allFailed, "One of test case passed parsing stage.");
    }

    @Test
    public void unsupportedDataSetError() throws ParseException {
        ArrayList<String[]> cmds = new ArrayList<>();
        String[] unsupportedDataSet = new String[] {
            "-pweb"
        };
        cmds.add(unsupportedDataSet);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        PrintStream stdErr = System.out;
        System.setErr(ps);
        App.main(unsupportedDataSet);
        System.setErr(stdErr);

        assertEquals("\"web\" data set is not supported yet!", baos.toString());
    }

    @Test
    public void parseCommondline() {
        try {
            App.main(new String[] {
                "-p=yttr"
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    
}
