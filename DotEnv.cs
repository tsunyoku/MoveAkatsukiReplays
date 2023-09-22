namespace MoveAkatsukiReplays;

public static class DotEnv
{
    private static string UnquoteString(string str)
    {
        if (string.IsNullOrEmpty(str))
            return str;
    
        var length = str.Length;
        if (length > 1 && str[0] == '\"' && str[length - 1] == '\"')
            str = str.Substring(1, length - 2);
    
        return str;
    }
    
    public static void Load(string filePath)
    {
        if (!File.Exists(filePath))
            return;
    
        foreach (var line in File.ReadAllLines(filePath))
        {
            var parts = line.Split(
                '=',
                StringSplitOptions.RemoveEmptyEntries);

            switch (parts.Length)
            {
                case < 2:
                    continue;
                case > 2:
                    parts[1] = string.Join('=', parts[1..]);
                    break;
            }

            Environment.SetEnvironmentVariable(parts[0], UnquoteString(parts[1]));
        }
    }
}