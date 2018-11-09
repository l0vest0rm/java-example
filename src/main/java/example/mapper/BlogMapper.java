package example.mapper;

import example.bean.Blog;
import org.apache.ibatis.annotations.Select;

public interface BlogMapper {
    @Select("SELECT * FROM m_users WHERE id = #{id}")
    Blog selectBlogWithAnnotations(Integer id);

    Blog selectBlog(Integer id);
}